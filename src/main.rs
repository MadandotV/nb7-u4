//! The fast pipeline. Stages, each on every core:
//!
//! 1. Parse: scan the CSVs with AVX2, turn each event line into a 32-byte
//!    record partitioned by a hash of its event ID, and each valid click into
//!    a record. Links are parsed first and joined on one thread meanwhile.
//! 2. Dedup each event partition, name the person of every surviving event
//!    and click, and partition both by person.
//! 3. Per person partition: group by person, sort by time, sessionize, credit
//!    and assist, and write `user_daily`.
//! 4. Merge the per-thread campaign counters and write `campaign_daily`.
//!
//! Anything the fast path does not represent exactly (an ingest time that is
//! not whole seconds, a window months long, hundreds of categories) sets
//! `BAIL`, and the slow implementation in `slow.rs` writes the answer instead.

use std::arch::x86_64::*;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

use shopsess::keys;
use shopsess::pqw::{self, Column, Kind, Spec};
use shopsess::slow;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: u64 = 1_800_000;
const BAD: i64 = i64::MIN;
const VALID: u32 = 1 << 31;
const NO_CAMPAIGN: u32 = u32::MAX;

static BAIL: AtomicBool = AtomicBool::new(false);

#[cold]
fn bail() {
    BAIL.store(true, Relaxed);
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
#[repr(C)]
struct Rec {
    eid: u64,
    user: u64,
    ts: u32,
    ingest: i32,
    /// `VALID`, and for a purchase the price in the low 24 bits and the
    /// category in bits 24 to 30.
    payload: u32,
    _pad: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    person: u64,
    ts: u32,
    payload: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Ck {
    /// The user in stage 1, the person from stage 2 on.
    person: u64,
    id: u64,
    /// Milliseconds since `window_start - 7 days`.
    ts: u32,
    campaign: u32,
}

// ---------------------------------------------------------------- files

struct Input {
    base: *const u8,
    /// Lines in `0..cut` end at least 64 bytes before the end of the file.
    cut: usize,
    /// The rest of the file, newline-terminated and padded.
    tail: Vec<u8>,
    tail_end: usize,
}

unsafe impl Sync for Input {}
unsafe impl Send for Input {}

fn csv_files(folder: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = fs::read_dir(folder)
        .map(|entries| entries.map(|entry| entry.unwrap().path()).collect())
        .unwrap_or_default();
    paths.retain(|path| path.extension().is_some_and(|ext| ext == "csv") && path.is_file());
    paths
}

fn open(path: &Path) -> Input {
    use std::os::fd::AsRawFd;
    let file = fs::File::open(path).unwrap();
    let len = file.metadata().unwrap().len() as usize;
    if len == 0 {
        return Input { base: std::ptr::null(), cut: 0, tail: vec![b'\n'; 64], tail_end: 0 };
    }
    let base = unsafe {
        libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, file.as_raw_fd(), 0)
    };
    assert!(base != libc::MAP_FAILED, "mmap failed");
    let base = base as *const u8;
    let bytes = unsafe { std::slice::from_raw_parts(base, len) };
    let mut cut = len.saturating_sub(64);
    while cut > 0 && bytes[cut - 1] != b'\n' {
        cut -= 1;
    }
    let mut tail = bytes[cut..].to_vec();
    if tail.last() != Some(&b'\n') {
        tail.push(b'\n');
    }
    let tail_end = tail.len();
    tail.extend_from_slice(&[b'\n'; 64]);
    Input { base, cut, tail, tail_end }
}

#[derive(Clone, Copy)]
struct Task {
    file: usize,
    from: usize,
    to: usize,
    tail: bool,
}

fn tasks(inputs: &[Input], chunk: usize) -> Vec<Task> {
    let mut out = Vec::new();
    for (file, input) in inputs.iter().enumerate() {
        let mut from = 0;
        while from < input.cut {
            let to = (from + chunk).min(input.cut);
            out.push(Task { file, from, to, tail: false });
            from = to;
        }
        if input.tail_end > 0 {
            out.push(Task { file, from: 0, to: input.tail_end, tail: true });
        }
    }
    out
}

/// Runs `line` on every line of the task, without the file's header.
#[inline(always)]
unsafe fn run_task(input: &Input, task: Task, line: impl FnMut(*const u8, usize, &[usize; 8], usize, u32)) {
    let (base, mut from, to) = if task.tail {
        (input.tail.as_ptr(), 0, task.to)
    } else {
        // A chunk owns the lines that start in it.
        let adjust = |at: usize| {
            if at == 0 || at >= input.cut {
                return at.min(input.cut);
            }
            let mut at = at - 1;
            while *input.base.add(at) != b'\n' {
                at += 1;
            }
            at + 1
        };
        (input.base, adjust(task.from), adjust(task.to))
    };
    if from == 0 && (!task.tail || input.cut == 0) {
        while *base.add(from) != b'\n' {
            from += 1;
        }
        from += 1;
    }
    if from < to {
        scan(base, from, to, line);
    }
}

/// Calls `line(base, line_start, ends, fields, quotes)` for each line of
/// `from..to`, which must be whole lines with 64 readable bytes after `to`.
/// `ends[k]` is where field `k` ends; `quotes` counts the line's quotes.
#[inline(always)]
unsafe fn scan(
    base: *const u8,
    from: usize,
    to: usize,
    mut line: impl FnMut(*const u8, usize, &[usize; 8], usize, u32),
) {
    let newline = _mm256_set1_epi8(b'\n' as i8);
    let comma = _mm256_set1_epi8(b',' as i8);
    let quote = _mm256_set1_epi8(b'"' as i8);
    let mut at = from;
    let mut line_start = from;
    let mut fields = 0usize;
    let mut ends = [0usize; 8];
    let mut quotes = 0u32;
    while at < to {
        let low = _mm256_loadu_si256(base.add(at) as *const __m256i);
        let high = _mm256_loadu_si256(base.add(at + 32) as *const __m256i);
        let mask = |needle: __m256i| {
            u64::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8(low, needle)) as u32)
                | u64::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8(high, needle)) as u32) << 32
        };
        let mut newlines = mask(newline);
        let mut separators = newlines | mask(comma);
        let mut quoted = mask(quote);
        if to - at < 64 {
            let keep = (1u64 << (to - at)) - 1;
            newlines &= keep;
            separators &= keep;
            quoted &= keep;
        }
        while separators != 0 {
            let offset = separators.trailing_zeros() as usize;
            let bit = separators & separators.wrapping_neg();
            *ends.get_unchecked_mut(fields & 7) = at + offset;
            fields += 1;
            if newlines & bit != 0 {
                let before = bit - 1;
                quotes += (quoted & before).count_ones();
                quoted &= !before;
                line(base, line_start, &ends, fields, quotes);
                line_start = at + offset + 1;
                fields = 0;
                quotes = 0;
            }
            separators &= separators - 1;
        }
        quotes += quoted.count_ones();
        at += 64;
    }
}

/// Field `index` of a line, with its wrapping quotes removed and counted.
#[inline(always)]
unsafe fn field(
    base: *const u8,
    line_start: usize,
    ends: &[usize; 8],
    index: usize,
    stripped: &mut u32,
) -> (*const u8, usize) {
    let from = if index == 0 { line_start } else { ends[index - 1] + 1 };
    let to = ends[index];
    if to - from >= 2 && *base.add(from) == b'"' && *base.add(to - 1) == b'"' {
        *stripped += 2;
        (base.add(from + 1), to - from - 2)
    } else {
        (base.add(from), to - from)
    }
}

// ---------------------------------------------------------------- timestamps

struct Calendar {
    year_month: u32,
    /// Days from the epoch to day 0 of the cached month.
    days: i64,
    days_in_month: u32,
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

/// `YYYY-MM-DDTHH:MM:SS` at `at` as epoch milliseconds, or `BAD`.
#[inline(always)]
unsafe fn civil(at: *const u8, calendar: &mut Calendar) -> i64 {
    let a = (at as *const u64).read_unaligned() ^ u64::from_le_bytes(*b"0000-00-");
    let b = (at.add(8) as *const u64).read_unaligned() ^ u64::from_le_bytes(*b"00T00:00");
    let c = u64::from((at.add(16) as *const u32).read_unaligned() & 0x00FF_FFFF) ^ u64::from_le_bytes(*b":00\0\0\0\0\0");
    // A digit byte must be 0 to 9 and a separator byte 0.
    let check = |value: u64, add: u64| ((value.wrapping_add(add)) | value) & 0x8080_8080_8080_8080;
    if check(a, 0x7F76_767F_7676_7676) | check(b, 0x7676_7F76_767F_7676) | check(c, 0x0000_0000_0076_767F) != 0 {
        return BAD;
    }
    let digit = |value: u64, index: u32| (value >> (8 * index)) as u32 & 0xFF;
    let year = digit(a, 0) * 1000 + digit(a, 1) * 100 + digit(a, 2) * 10 + digit(a, 3);
    let month = digit(a, 5) * 10 + digit(a, 6);
    let day = digit(b, 0) * 10 + digit(b, 1);
    let hour = digit(b, 3) * 10 + digit(b, 4);
    let minute = digit(b, 6) * 10 + digit(b, 7);
    let second = digit(c, 1) * 10 + digit(c, 2);
    let year_month = year * 16 + month;
    if year_month != calendar.year_month {
        if !(1..=12).contains(&month) {
            return BAD;
        }
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        calendar.year_month = year_month;
        calendar.days = days_from_civil(i64::from(year), i64::from(month), 1) - 1;
        calendar.days_in_month = match month {
            4 | 6 | 9 | 11 => 30,
            2 => 28 + u32::from(leap),
            _ => 31,
        };
    }
    if day == 0 || day > calendar.days_in_month || hour > 23 || minute > 59 || second > 59 {
        return BAD;
    }
    (calendar.days + i64::from(day)) * DAY + i64::from(hour * 3600 + minute * 60 + second) * 1000
}

/// A `ts` field as epoch milliseconds, or `BAD`.
#[inline(always)]
unsafe fn timestamp(at: *const u8, len: usize, calendar: &mut Calendar) -> i64 {
    if len == 20 && *at.add(19) == b'Z' {
        return civil(at, calendar);
    }
    if len == 25 {
        let sign = *at.add(19);
        if sign == b'+' || sign == b'-' {
            let digit = |index: usize| u32::from((*at.add(index)).wrapping_sub(b'0'));
            let (h1, h2, m1, m2) = (digit(20), digit(21), digit(23), digit(24));
            if *at.add(22) != b':' || h1 > 9 || h2 > 9 || m1 > 9 || m2 > 9 {
                return BAD;
            }
            let (hours, minutes) = (h1 * 10 + h2, m1 * 10 + m2);
            if hours > 23 || minutes > 59 {
                return BAD;
            }
            let local = civil(at, calendar);
            if local == BAD {
                return BAD;
            }
            let offset = i64::from(hours * 60 + minutes) * 60_000;
            return if sign == b'+' { local - offset } else { local + offset };
        }
    }
    if len == 0 {
        return BAD;
    }
    let mut value = 0u64;
    for index in 0..len {
        let digit = u64::from((*at.add(index)).wrapping_sub(b'0'));
        if digit > 9 {
            return BAD;
        }
        value = (value * 10 + digit).min(1 << 60);
    }
    value as i64
}

// ---------------------------------------------------------------- context

struct Context {
    window_start: i64,
    window_end: i64,
    /// `window_start - 7 days`.
    click_start: i64,
    /// `window_start` minus the midnight before it.
    day_offset: u64,
    first_day: i32,
    days: usize,
    /// Product to category index, for purchases.
    products: HashMap<Vec<u8>, u32>,
    /// Categories in byte order, `unknown` among them.
    categories: Vec<Vec<u8>>,
    unknown: u32,
    campaign_ids: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
    /// `c_N` to campaign index, for canonical `N` below the table's length.
    campaign_by_number: Vec<u32>,
    campaign_by_name: HashMap<Vec<u8>, u32>,
    event_bits: u32,
    person_bits: u32,
}

impl Context {
    #[inline(always)]
    unsafe fn campaign(&self, at: *const u8, len: usize) -> u32 {
        if (3..=8).contains(&len) && *at == b'c' && *at.add(1) == b'_' && *at.add(2) != b'0' {
            let mut number = 0usize;
            let mut ok = true;
            for index in 2..len {
                let digit = (*at.add(index)).wrapping_sub(b'0');
                ok &= digit <= 9;
                number = number * 10 + usize::from(digit);
            }
            if ok && number < self.campaign_by_number.len() {
                return self.campaign_by_number[number];
            }
        }
        self.campaign_by_name.get(std::slice::from_raw_parts(at, len)).copied().unwrap_or(NO_CAMPAIGN)
    }
}

fn canonical_campaign(id: &[u8]) -> Option<usize> {
    if (3..=8).contains(&id.len()) && id.starts_with(b"c_") && id[2] != b'0' && id[2..].iter().all(u8::is_ascii_digit) {
        return std::str::from_utf8(&id[2..]).unwrap().parse().ok();
    }
    None
}

// ---------------------------------------------------------------- stage 1

struct Parsed {
    events: Vec<Vec<Rec>>,
    clicks: Vec<Ck>,
}

#[inline(always)]
unsafe fn event_line(
    context: &Context,
    calendar: &mut Calendar,
    out: &mut [Vec<Rec>],
    base: *const u8,
    line_start: usize,
    ends: &[usize; 8],
    fields: usize,
    quotes: u32,
) {
    if fields != 7 {
        return;
    }
    let mut stripped = 0;
    let (id, id_len) = field(base, line_start, ends, 0, &mut stripped);
    let (user, user_len) = field(base, line_start, ends, 1, &mut stripped);
    let (ts, ts_len) = field(base, line_start, ends, 2, &mut stripped);
    let (ingest, ingest_len) = field(base, line_start, ends, 3, &mut stripped);
    let (kind, kind_len) = field(base, line_start, ends, 4, &mut stripped);
    let (product, product_len) = field(base, line_start, ends, 5, &mut stripped);
    let (price, price_len) = field(base, line_start, ends, 6, &mut stripped);
    if stripped != quotes || id_len == 0 {
        return;
    }
    let ingest = timestamp(ingest, ingest_len, calendar);
    let ingest_seconds = (ingest - context.window_start) / 1000;
    if ingest == BAD || ingest % 1000 != 0 || ingest_seconds != i64::from(ingest_seconds as i32) {
        return bail();
    }
    let mut record = Rec { eid: keys::pack(id, id_len), user: 0, ts: 0, ingest: ingest_seconds as i32, payload: 0, _pad: 0 };
    'valid: {
        if user_len == 0 || product_len == 0 {
            break 'valid;
        }
        let ts = timestamp(ts, ts_len, calendar);
        if ts < context.window_start || ts >= context.window_end || ingest - ts > DAY {
            break 'valid;
        }
        let mut payload = VALID;
        if kind_len == 4 {
            let word = (kind as *const u32).read_unaligned();
            if word != u32::from_le_bytes(*b"view") && word != u32::from_le_bytes(*b"cart") {
                break 'valid;
            }
        } else if kind_len == 8 && (kind as *const u64).read_unaligned() == u64::from_le_bytes(*b"purchase") {
            if price_len == 0 {
                break 'valid;
            }
            let mut value = 0u64;
            for index in 0..price_len {
                let digit = u64::from((*price.add(index)).wrapping_sub(b'0'));
                if digit > 9 {
                    break 'valid;
                }
                value = (value * 10 + digit).min(1 << 40);
            }
            if !(1..=10_000_000).contains(&value) {
                break 'valid;
            }
            let category = context
                .products
                .get(std::slice::from_raw_parts(product, product_len))
                .copied()
                .unwrap_or(context.unknown);
            payload |= category << 24 | value as u32;
        } else {
            break 'valid;
        }
        record.user = keys::pack(user, user_len);
        record.ts = (ts - context.window_start) as u32;
        record.payload = payload;
    }
    let part = (keys::hash(record.eid) >> (64 - context.event_bits)) as usize;
    out.get_unchecked_mut(part).push(record);
}

#[inline(always)]
unsafe fn click_line(
    context: &Context,
    calendar: &mut Calendar,
    out: &mut Vec<Ck>,
    base: *const u8,
    line_start: usize,
    ends: &[usize; 8],
    fields: usize,
    quotes: u32,
) {
    if fields != 4 {
        return;
    }
    let mut stripped = 0;
    let (id, id_len) = field(base, line_start, ends, 0, &mut stripped);
    let (user, user_len) = field(base, line_start, ends, 1, &mut stripped);
    let (campaign, campaign_len) = field(base, line_start, ends, 2, &mut stripped);
    let (ts, ts_len) = field(base, line_start, ends, 3, &mut stripped);
    if stripped != quotes || id_len == 0 || user_len == 0 {
        return;
    }
    let campaign = context.campaign(campaign, campaign_len);
    if campaign == NO_CAMPAIGN {
        return;
    }
    let ts = timestamp(ts, ts_len, calendar);
    if ts < context.click_start || ts >= context.window_end {
        return;
    }
    out.push(Ck {
        person: keys::pack(user, user_len),
        id: keys::pack(id, id_len),
        ts: (ts - context.click_start) as u32,
        campaign,
    });
}

#[inline(always)]
unsafe fn link_line(
    out: &mut Vec<(u64, u64)>,
    base: *const u8,
    line_start: usize,
    ends: &[usize; 8],
    fields: usize,
    quotes: u32,
) {
    if fields != 2 {
        return;
    }
    let mut stripped = 0;
    let (a, a_len) = field(base, line_start, ends, 0, &mut stripped);
    let (b, b_len) = field(base, line_start, ends, 1, &mut stripped);
    if stripped != quotes || a_len == 0 || b_len == 0 {
        return;
    }
    let (a, b) = (keys::pack(a, a_len), keys::pack(b, b_len));
    if a != b {
        out.push((a, b));
    }
}

// ---------------------------------------------------------------- links

struct Links {
    /// `[user, person]` slots, open addressing; a user of 0 is an empty slot.
    slots: Vec<[u64; 2]>,
    mask: usize,
}

impl Links {
    #[inline(always)]
    fn slot(&self, user: u64) -> usize {
        (keys::hash(user) >> 20) as usize & self.mask
    }

    #[inline(always)]
    fn person(&self, user: u64) -> u64 {
        let mut at = self.slot(user);
        loop {
            let slot = unsafe { self.slots.get_unchecked(at) };
            if slot[0] == user {
                return slot[1];
            }
            if slot[0] == 0 {
                return user;
            }
            at = (at + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn prefetch(&self, user: u64) {
        unsafe { _mm_prefetch(self.slots.as_ptr().add(self.slot(user)) as *const i8, _MM_HINT_T0) };
    }

    fn build(pairs: &[Vec<(u64, u64)>]) -> Links {
        let total: usize = pairs.iter().map(Vec::len).sum();
        let capacity = (total * 4).max(16).next_power_of_two();
        let mut links = Links { slots: vec![[0u64; 2]; capacity], mask: capacity - 1 };
        let mut parent: Vec<u32> = Vec::with_capacity(total * 2);
        let mut name: Vec<u64> = Vec::with_capacity(total * 2);
        let mut index = |links: &mut Links, parent: &mut Vec<u32>, name: &mut Vec<u64>, user: u64| -> u32 {
            let mut at = links.slot(user);
            loop {
                let slot = &mut links.slots[at];
                if slot[0] == user {
                    return slot[1] as u32;
                }
                if slot[0] == 0 {
                    let id = parent.len() as u32;
                    *slot = [user, u64::from(id)];
                    parent.push(id);
                    name.push(user);
                    return id;
                }
                at = (at + 1) & links.mask;
            }
        };
        fn root(parent: &mut [u32], mut at: u32) -> u32 {
            loop {
                let up = parent[at as usize];
                if up == at {
                    return at;
                }
                let above = parent[up as usize];
                parent[at as usize] = above;
                at = above;
            }
        }
        for list in pairs {
            for &(a, b) in list {
                let a = index(&mut links, &mut parent, &mut name, a);
                let b = index(&mut links, &mut parent, &mut name, b);
                let (a, b) = (root(&mut parent, a), root(&mut parent, b));
                if a != b {
                    parent[a as usize] = b;
                    if keys::compare(name[a as usize], name[b as usize]).is_lt() {
                        name[b as usize] = name[a as usize];
                    }
                }
            }
        }
        for slot in &mut links.slots {
            if slot[0] != 0 {
                slot[1] = name[root(&mut parent, slot[1] as u32) as usize];
            }
        }
        links
    }
}

// ---------------------------------------------------------------- stage 2

struct Shuffled {
    events: Vec<Vec<Ev>>,
    clicks: Vec<Vec<Ck>>,
}

/// Dedups one event partition and sends the surviving events to their person's partition.
fn dedup(context: &Context, links: &Links, parts: &[&[Rec]], table: &mut Vec<*const Rec>, out: &mut [Vec<Ev>]) {
    let total: usize = parts.iter().map(|part| part.len()).sum();
    if total == 0 {
        return;
    }
    let capacity = (total * 2).next_power_of_two();
    table.clear();
    table.resize(capacity, std::ptr::null());
    let mask = capacity - 1;
    let shift = 64 - context.event_bits - capacity.trailing_zeros();
    for part in parts {
        for record in *part {
            let mut at = (keys::hash(record.eid) >> shift) as usize & mask;
            loop {
                let slot = unsafe { table.get_unchecked_mut(at) };
                if slot.is_null() {
                    *slot = record;
                    break;
                }
                let kept = unsafe { &**slot };
                if kept.eid == record.eid {
                    if record.ingest < kept.ingest {
                        *slot = record;
                    }
                    break;
                }
                at = (at + 1) & mask;
            }
        }
    }
    let shift = 64 - context.person_bits;
    let mut batch = [(0u64, 0u32, 0u32); 32];
    let mut filled = 0;
    let mut flush = |batch: &[(u64, u32, u32)]| {
        for &(user, ts, payload) in batch {
            let person = links.person(user);
            let part = (keys::hash(person) >> shift) as usize;
            unsafe { out.get_unchecked_mut(part) }.push(Ev { person, ts, payload });
        }
    };
    for &slot in table.iter() {
        if slot.is_null() {
            continue;
        }
        let record = unsafe { &*slot };
        if record.payload & VALID != 0 {
            links.prefetch(record.user);
            batch[filled] = (record.user, record.ts, record.payload);
            filled += 1;
            if filled == batch.len() {
                flush(&batch);
                filled = 0;
            }
        }
    }
    flush(&batch[..filled]);
}

fn shuffle_clicks(context: &Context, links: &Links, clicks: &[Ck], out: &mut [Vec<Ck>]) {
    let shift = 64 - context.person_bits;
    for chunk in clicks.chunks(32) {
        for click in chunk {
            links.prefetch(click.person);
        }
        for click in chunk {
            let person = links.person(click.person);
            let part = (keys::hash(person) >> shift) as usize;
            unsafe { out.get_unchecked_mut(part) }.push(Ck { person, ..*click });
        }
    }
}

// ---------------------------------------------------------------- stage 3

#[derive(Clone, Copy, Default)]
struct Tally {
    revenue: i64,
    sessions: u32,
    purchasing: u32,
    assisted: u32,
}

const USER_DAILY: [Spec; 6] = [
    Spec { name: "user_id", kind: Kind::Str, optional: false },
    Spec { name: "day", kind: Kind::Date, optional: false },
    Spec { name: "sessions", kind: Kind::I32, optional: false },
    Spec { name: "events", kind: Kind::I32, optional: false },
    Spec { name: "revenue_cents", kind: Kind::I64, optional: false },
    Spec { name: "top_category", kind: Kind::Str, optional: true },
];

struct Worker<'a> {
    context: &'a Context,
    output: &'a Path,
    thread: usize,
    files: usize,
    rows: usize,
    columns: Vec<Column>,
    /// `[campaign * days + day]`, `organic` last.
    tallies: Vec<Tally>,
    // Person grouping.
    keys: Vec<u64>,
    ids: Vec<u32>,
    people: Vec<u64>,
    counts: Vec<u32>,
    click_counts: Vec<u32>,
    owner: Vec<u32>,
    events: Vec<u64>,
    clicks: Vec<Ck>,
    // Campaigns clicked in the 7 days before the current session.
    active: Vec<u32>,
    active_count: Vec<u32>,
    active_at: Vec<u32>,
    // Purchases by category for the current row.
    category_sum: Vec<i64>,
    category_used: Vec<u32>,
}

struct Row {
    day: usize,
    sessions: u32,
    events: u32,
    revenue: i64,
}

impl<'a> Worker<'a> {
    fn new(context: &'a Context, output: &'a Path, thread: usize) -> Self {
        let campaigns = context.campaign_ids.len();
        Worker {
            context,
            output,
            thread,
            files: 0,
            rows: 0,
            columns: (0..6).map(|_| Column::default()).collect(),
            tallies: vec![Tally::default(); (campaigns + 1) * context.days],
            keys: Vec::new(),
            ids: Vec::new(),
            people: Vec::new(),
            counts: Vec::new(),
            click_counts: Vec::new(),
            owner: Vec::new(),
            events: Vec::new(),
            clicks: Vec::new(),
            active: Vec::new(),
            active_count: vec![0; campaigns],
            active_at: vec![0; campaigns],
            category_sum: vec![0; context.categories.len()],
            category_used: Vec::new(),
        }
    }

    fn write_file(&mut self) {
        if self.rows == 0 && self.files > 0 {
            return;
        }
        let path = self.output.join("user_daily").join(format!("part-{}-{}.parquet", self.thread, self.files));
        pqw::write(&path, &USER_DAILY, &self.columns, self.rows).unwrap();
        self.files += 1;
        self.rows = 0;
        for column in &mut self.columns {
            column.values.clear();
            column.present.clear();
        }
    }

    #[inline(always)]
    fn emit(&mut self, person: u64, row: &Row) {
        keys::unpack_string(person, &mut self.columns[0].values);
        self.columns[1].push_i32(self.context.first_day + row.day as i32);
        self.columns[2].push_i32(row.sessions as i32);
        self.columns[3].push_i32(row.events as i32);
        self.columns[4].push_i64(row.revenue);
        let mut top = u32::MAX;
        let mut best = 0;
        for &category in &self.category_used {
            let sum = std::mem::take(&mut self.category_sum[category as usize]);
            if sum > best || (sum == best && category < top) {
                best = sum;
                top = category;
            }
        }
        self.category_used.clear();
        self.columns[5].push_present(self.rows, top != u32::MAX);
        if top != u32::MAX {
            self.columns[5].push_str(&self.context.categories[top as usize]);
        }
        self.rows += 1;
        if self.rows >= 1 << 20 {
            self.write_file();
        }
    }

    /// One person partition: `events` and `clicks` are the partition's slices from every thread.
    fn partition(&mut self, events: &[&[Ev]], clicks: &[&[Ck]]) {
        let total: usize = events.iter().map(|part| part.len()).sum();
        if total == 0 {
            return;
        }
        // Group the events by person.
        let capacity = (total * 2).next_power_of_two();
        let mask = capacity - 1;
        let shift = 64 - self.context.person_bits - capacity.trailing_zeros().min(64 - self.context.person_bits);
        self.keys.clear();
        self.keys.resize(capacity, 0);
        self.ids.resize(capacity, 0);
        self.people.clear();
        self.counts.clear();
        self.owner.clear();
        for part in events {
            for event in *part {
                let mut at = (keys::hash(event.person) >> shift) as usize & mask;
                let id = loop {
                    let key = unsafe { self.keys.get_unchecked_mut(at) };
                    if *key == event.person {
                        break self.ids[at];
                    }
                    if *key == 0 {
                        *key = event.person;
                        let id = self.people.len() as u32;
                        self.ids[at] = id;
                        self.people.push(event.person);
                        self.counts.push(0);
                        break id;
                    }
                    at = (at + 1) & mask;
                };
                self.counts[id as usize] += 1;
                self.owner.push(id);
            }
        }
        let people = self.people.len();
        // counts[p] becomes where person p's events end, then where they start.
        let mut sum = 0;
        for count in &mut self.counts {
            sum += *count;
            *count = sum;
        }
        self.events.resize(total, 0);
        let mut next = 0;
        for part in events {
            for event in *part {
                let id = self.owner[next] as usize;
                next += 1;
                self.counts[id] -= 1;
                self.events[self.counts[id] as usize] = u64::from(event.ts) << 32 | u64::from(event.payload);
            }
        }
        self.counts.push(total as u32);

        // Group the clicks of those people the same way.
        self.click_counts.clear();
        self.click_counts.resize(people, 0);
        self.owner.clear();
        for part in clicks {
            for click in *part {
                let mut at = (keys::hash(click.person) >> shift) as usize & mask;
                let id = loop {
                    let key = self.keys[at];
                    if key == click.person {
                        break self.ids[at];
                    }
                    if key == 0 {
                        break u32::MAX;
                    }
                    at = (at + 1) & mask;
                };
                if id != u32::MAX {
                    self.click_counts[id as usize] += 1;
                }
                self.owner.push(id);
            }
        }
        let mut sum = 0;
        for count in &mut self.click_counts {
            sum += *count;
            *count = sum;
        }
        let click_total = sum as usize;
        self.clicks.clear();
        self.clicks.resize(click_total, Ck { person: 0, id: 0, ts: 0, campaign: 0 });
        let mut next = 0;
        for part in clicks {
            for click in *part {
                let id = self.owner[next];
                next += 1;
                if id != u32::MAX {
                    self.click_counts[id as usize] -= 1;
                    self.clicks[self.click_counts[id as usize] as usize] = *click;
                }
            }
        }
        self.click_counts.push(click_total as u32);

        for person in 0..people {
            let events = self.counts[person] as usize..self.counts[person + 1] as usize;
            let clicks = self.click_counts[person] as usize..self.click_counts[person + 1] as usize;
            self.person(person, events, clicks);
        }
    }

    fn person(&mut self, person: usize, events: std::ops::Range<usize>, clicks: std::ops::Range<usize>) {
        let name = self.people[person];
        let days = self.context.days;
        let organic = self.context.campaign_ids.len();
        let events = unsafe { std::slice::from_raw_parts_mut(self.events.as_mut_ptr().add(events.start), events.len()) };
        let clicks = unsafe { std::slice::from_raw_parts_mut(self.clicks.as_mut_ptr().add(clicks.start), clicks.len()) };
        if events.len() > 1 {
            events.sort_unstable();
        }
        if clicks.len() > 1 {
            clicks.sort_unstable_by_key(|click| click.ts);
        }
        let mut row = Row { day: usize::MAX, sessions: 0, events: 0, revenue: 0 };
        // Clicks `low..high` are those in the 7 days up to the session's start.
        let (mut low, mut high) = (0, 0);
        let mut at = 0;
        while at < events.len() {
            let start = events[at] >> 32;
            let day = ((self.context.day_offset + start) / DAY as u64) as usize;
            if day != row.day {
                if row.day != usize::MAX {
                    self.emit(name, &row);
                }
                row = Row { day, sessions: 0, events: 0, revenue: 0 };
            }
            let mut revenue = 0i64;
            let mut last = start;
            let first = at;
            while at < events.len() {
                let ts = events[at] >> 32;
                if ts - last > GAP {
                    break;
                }
                last = ts;
                let price = events[at] as u32 & 0x00FF_FFFF;
                if price != 0 {
                    let category = (events[at] as u32 >> 24 & 0x7F) as usize;
                    revenue += i64::from(price);
                    if self.category_sum[category] == 0 {
                        self.category_used.push(category as u32);
                    }
                    self.category_sum[category] += i64::from(price);
                }
                at += 1;
            }
            row.sessions += 1;
            row.events += (at - first) as u32;
            row.revenue += revenue;

            // In click time the session starts at `start + 7 days`.
            let reach = (start + WEEK as u64) as u32;
            while high < clicks.len() && clicks[high].ts <= reach {
                let campaign = clicks[high].campaign as usize;
                if self.active_count[campaign] == 0 {
                    self.active_at[campaign] = self.active.len() as u32;
                    self.active.push(campaign as u32);
                }
                self.active_count[campaign] += 1;
                high += 1;
            }
            while low < high && clicks[low].ts < start as u32 {
                let campaign = clicks[low].campaign as usize;
                self.active_count[campaign] -= 1;
                if self.active_count[campaign] == 0 {
                    let hole = self.active_at[campaign] as usize;
                    let moved = self.active.pop().unwrap();
                    if moved as usize != campaign {
                        self.active[hole] = moved;
                        self.active_at[moved as usize] = hole as u32;
                    }
                }
                low += 1;
            }
            let mut credited = organic;
            if low < high {
                let mut chosen = high - 1;
                let mut other = chosen;
                while other > low && clicks[other - 1].ts == clicks[chosen].ts {
                    other -= 1;
                    if keys::compare(clicks[other].id, clicks[chosen].id).is_lt() {
                        chosen = other;
                    }
                }
                credited = clicks[chosen].campaign as usize;
            }
            let tally = &mut self.tallies[credited * days + day];
            tally.sessions += 1;
            tally.purchasing += u32::from(revenue != 0);
            tally.revenue += revenue;
            for &campaign in &self.active {
                self.tallies[campaign as usize * days + day].assisted += 1;
            }
        }
        if row.day != usize::MAX {
            self.emit(name, &row);
        }
        for &campaign in &self.active {
            self.active_count[campaign as usize] = 0;
        }
        self.active.clear();
    }
}

// ---------------------------------------------------------------- main

fn load_context(data: &Path, event_bytes: u64) -> Option<Context> {
    let (window_start, window_end) = slow::manifest(&data.join("manifest.json"));
    if window_end <= window_start || window_end - window_start + WEEK >= 1 << 31 {
        return None;
    }
    let mut categories: Vec<Vec<u8>> = vec![b"unknown".to_vec()];
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for line in slow::file_lines(&data.join("products.csv")) {
        if let Some(row) = slow::fields(&line, 2) {
            rows.push((row[0].to_vec(), row[1].to_vec()));
            categories.push(row[1].to_vec());
        }
    }
    categories.sort();
    categories.dedup();
    if categories.len() > 127 {
        return None;
    }
    let category = |name: &[u8]| categories.binary_search_by(|probe| probe.as_slice().cmp(name)).unwrap() as u32;
    let products: HashMap<Vec<u8>, u32> = rows.iter().map(|(id, name)| (id.clone(), category(name))).collect();
    let unknown = category(b"unknown");

    let mut campaign_ids = Vec::new();
    let mut channels = Vec::new();
    let mut campaign_by_name = HashMap::new();
    for line in slow::file_lines(&data.join("campaigns.csv")) {
        if let Some(row) = slow::fields(&line, 2) {
            if !campaign_by_name.contains_key(row[0]) {
                campaign_by_name.insert(row[0].to_vec(), campaign_ids.len() as u32);
                campaign_ids.push(row[0].to_vec());
                channels.push(row[1].to_vec());
            }
        }
    }
    if campaign_ids.len() > 1 << 20 {
        return None;
    }
    let numbers: Vec<Option<usize>> = campaign_ids.iter().map(|id| canonical_campaign(id)).collect();
    let size = numbers.iter().flatten().max().map_or(0, |&most| most + 1).min(1 << 20);
    let mut campaign_by_number = vec![NO_CAMPAIGN; size];
    for (index, number) in numbers.iter().enumerate() {
        if let Some(number) = number {
            if *number < size {
                campaign_by_number[*number] = index as u32;
            }
        }
    }

    let first_day = window_start.div_euclid(DAY);
    let estimate = event_bytes / 80;
    let bits = |per: u64| (estimate / per).max(1).next_power_of_two().trailing_zeros().clamp(4, 12);
    Some(Context {
        window_start,
        window_end,
        click_start: window_start - WEEK,
        day_offset: (window_start - first_day * DAY) as u64,
        first_day: first_day as i32,
        days: ((window_end - 1).div_euclid(DAY) - first_day + 1) as usize,
        products,
        categories,
        unknown,
        campaign_ids,
        channels,
        campaign_by_number,
        campaign_by_name,
        event_bits: bits(40_000),
        person_bits: bits(40_000),
    })
}

fn main() {
    let started = Instant::now();
    let arguments: Vec<String> = std::env::args().collect();
    let pick = |at: usize, name: &str, default: &str| {
        if arguments.len() >= 3 {
            PathBuf::from(&arguments[arguments.len() - 2 + at])
        } else {
            PathBuf::from(std::env::var(name).unwrap_or(default.to_string()))
        }
    };
    let data = pick(0, "DATA_DIR", "/data");
    let output = pick(1, "OUTPUT_DIR", "/output");
    let threads = std::env::var("NCPU")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&count| count > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from));
    let verbose = std::env::var_os("SHOPSESS_QUIET").is_none();
    let lap = |what: &str| {
        if verbose {
            eprintln!("{:>8.1} ms  {what}", started.elapsed().as_secs_f64() * 1e3);
        }
    };

    fs::create_dir_all(output.join("user_daily")).unwrap();
    fs::create_dir_all(output.join("campaign_daily")).unwrap();

    let link_inputs: Vec<Input> = csv_files(&data.join("links")).iter().map(|path| open(path)).collect();
    let click_inputs: Vec<Input> = csv_files(&data.join("clicks")).iter().map(|path| open(path)).collect();
    let event_inputs: Vec<Input> = csv_files(&data.join("events")).iter().map(|path| open(path)).collect();
    let event_bytes: u64 = event_inputs.iter().map(|input| (input.cut + input.tail_end) as u64).sum();
    let Some(context) = load_context(&data, event_bytes) else {
        return slow::run(&data, &output);
    };
    let context = &context;
    lap("opened");

    // Stage 1: links, then clicks and events while one thread joins the links.
    const CHUNK: usize = 1 << 20;
    let link_tasks = tasks(&link_inputs, CHUNK);
    let next = AtomicUsize::new(0);
    let pairs: Vec<Vec<(u64, u64)>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let mut out = Vec::new();
                    while let Some(&task) = link_tasks.get(next.fetch_add(1, Relaxed)) {
                        unsafe {
                            run_task(&link_inputs[task.file], task, |base, start, ends, fields, quotes| {
                                link_line(&mut out, base, start, ends, fields, quotes)
                            });
                        }
                    }
                    out
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    lap("links parsed");

    let click_tasks = tasks(&click_inputs, CHUNK);
    let event_tasks = tasks(&event_inputs, CHUNK);
    let (next_click, next_event) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let event_parts = 1usize << context.event_bits;
    let reserve = (event_bytes / 70 / threads as u64 / event_parts as u64) as usize * 5 / 4 + 16;
    let (links, parsed): (Links, Vec<Parsed>) = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|thread| {
                let pairs = &pairs;
                let (click_tasks, event_tasks, click_inputs, event_inputs) =
                    (&click_tasks, &event_tasks, &click_inputs, &event_inputs);
                let (next_click, next_event) = (&next_click, &next_event);
                scope.spawn(move || {
                    let links = (thread == 0).then(|| Links::build(pairs));
                    let mut calendar = Calendar { year_month: 0, days: 0, days_in_month: 0 };
                    let mut parsed = Parsed {
                        events: (0..event_parts).map(|_| Vec::with_capacity(reserve)).collect(),
                        clicks: Vec::new(),
                    };
                    while let Some(&task) = event_tasks.get(next_event.fetch_add(1, Relaxed)) {
                        unsafe {
                            run_task(&event_inputs[task.file], task, |base, start, ends, fields, quotes| {
                                event_line(context, &mut calendar, &mut parsed.events, base, start, ends, fields, quotes)
                            });
                        }
                    }
                    while let Some(&task) = click_tasks.get(next_click.fetch_add(1, Relaxed)) {
                        unsafe {
                            run_task(&click_inputs[task.file], task, |base, start, ends, fields, quotes| {
                                click_line(context, &mut calendar, &mut parsed.clicks, base, start, ends, fields, quotes)
                            });
                        }
                    }
                    (links, parsed)
                })
            })
            .collect();
        let mut links = None;
        let mut all = Vec::new(); // per thread
        for handle in handles {
            let (built, parsed) = handle.join().unwrap();
            links = links.or(built);
            all.push(parsed);
        }
        (links.unwrap(), all)
    });
    lap("parsed");
    if BAIL.load(Relaxed) {
        return slow::run(&data, &output);
    }

    // Stage 2: dedup events, name people, partition by person.
    let person_parts = 1usize << context.person_bits;
    let next = AtomicUsize::new(0);
    const CLICK_CHUNK: usize = 1 << 16;
    let click_jobs: Vec<(usize, usize)> = parsed
        .iter()
        .enumerate()
        .flat_map(|(thread, parsed)| (0..parsed.clicks.len().div_ceil(CLICK_CHUNK)).map(move |chunk| (thread, chunk)))
        .collect();
    let next_click = AtomicUsize::new(0);
    let shuffled: Vec<Shuffled> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let mut out = Shuffled {
                        events: (0..person_parts).map(|_| Vec::new()).collect(),
                        clicks: (0..person_parts).map(|_| Vec::new()).collect(),
                    };
                    let mut table = Vec::new();
                    loop {
                        let part = next.fetch_add(1, Relaxed);
                        if part >= event_parts {
                            break;
                        }
                        let parts: Vec<&[Rec]> = parsed.iter().map(|parsed| parsed.events[part].as_slice()).collect();
                        dedup(context, &links, &parts, &mut table, &mut out.events);
                    }
                    while let Some(&(thread, chunk)) = click_jobs.get(next_click.fetch_add(1, Relaxed)) {
                        let clicks = &parsed[thread].clicks;
                        let from = chunk * CLICK_CHUNK;
                        let to = (from + CLICK_CHUNK).min(clicks.len());
                        shuffle_clicks(context, &links, &clicks[from..to], &mut out.clicks);
                    }
                    out
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    lap("shuffled");

    // Stage 3: sessions per person partition.
    let next = AtomicUsize::new(0);
    let tallies: Vec<Vec<Tally>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|thread| {
                let (shuffled, next, output) = (&shuffled, &next, &output);
                scope.spawn(move || {
                    let mut worker = Worker::new(context, output, thread);
                    loop {
                        let part = next.fetch_add(1, Relaxed);
                        if part >= person_parts {
                            break;
                        }
                        let events: Vec<&[Ev]> = shuffled.iter().map(|shuffled| shuffled.events[part].as_slice()).collect();
                        let clicks: Vec<&[Ck]> = shuffled.iter().map(|shuffled| shuffled.clicks[part].as_slice()).collect();
                        worker.partition(&events, &clicks);
                    }
                    worker.write_file();
                    worker.tallies
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    lap("sessions");

    // Stage 4: campaign_daily.
    let campaigns = context.campaign_ids.len();
    let mut columns: Vec<Column> = (0..7).map(|_| Column::default()).collect();
    let mut rows = 0;
    for campaign in 0..=campaigns {
        for day in 0..context.days {
            let mut total = Tally::default();
            for list in &tallies {
                let tally = &list[campaign * context.days + day];
                total.sessions += tally.sessions;
                total.purchasing += tally.purchasing;
                total.revenue += tally.revenue;
                total.assisted += tally.assisted;
            }
            if total.sessions == 0 && total.assisted == 0 {
                continue;
            }
            let (id, channel): (&[u8], &[u8]) = if campaign == campaigns {
                (b"organic", b"organic")
            } else {
                (&context.campaign_ids[campaign], &context.channels[campaign])
            };
            columns[0].push_str(id);
            columns[1].push_str(channel);
            columns[2].push_i32(context.first_day + day as i32);
            columns[3].push_i64(i64::from(total.sessions));
            columns[4].push_i64(i64::from(total.purchasing));
            columns[5].push_i64(total.revenue);
            columns[6].push_i64(i64::from(total.assisted));
            rows += 1;
        }
    }
    pqw::write(&output.join("campaign_daily").join("part-0.parquet"), &pqw::CAMPAIGN_DAILY, &columns, rows).unwrap();
    lap("done");
    // Skip the destructors: unmapping and freeing gigabytes is time on the clock.
    unsafe { libc::_exit(0) };
}
