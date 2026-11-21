//! Reading the CSVs: memory-mapped files cut into tasks, and a SIMD pass that
//! indexes the separators of a block before its lines are handled.

use std::arch::x86_64::*;
use std::fs;
use std::path::Path;

pub struct Input {
    base: *const u8,
    len: usize,
    /// Lines in `0..cut` end at least 64 bytes before the end of the file.
    cut: usize,
    /// A newline, the rest of the file, a newline if it lacked one, and padding.
    tail: Vec<u8>,
    tail_end: usize,
}

unsafe impl Sync for Input {}
unsafe impl Send for Input {}

impl Input {
    pub fn len(&self) -> u64 {
        self.len as u64
    }
}

pub fn open(path: &Path) -> Input {
    use std::os::fd::AsRawFd;
    let file = fs::File::open(path).unwrap();
    let len = file.metadata().unwrap().len() as usize;
    if len == 0 {
        return Input { base: std::ptr::null(), len: 0, cut: 0, tail: Vec::new(), tail_end: 0 };
    }
    let base = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, file.as_raw_fd(), 0) };
    assert!(base != libc::MAP_FAILED, "mmap failed");
    let base = base as *const u8;
    let bytes = unsafe { std::slice::from_raw_parts(base, len) };
    let mut cut = len.saturating_sub(64);
    while cut > 0 && bytes[cut - 1] != b'\n' {
        cut -= 1;
    }
    let mut tail = Vec::with_capacity(len - cut + 66);
    tail.push(b'\n');
    tail.extend_from_slice(&bytes[cut..]);
    if tail.last() != Some(&b'\n') {
        tail.push(b'\n');
    }
    let tail_end = tail.len();
    tail.extend_from_slice(&[b'\n'; 64]);
    Input { base, len, cut, tail, tail_end }
}

#[derive(Clone, Copy)]
pub struct Task {
    pub file: usize,
    from: usize,
    to: usize,
    tail: bool,
}

pub fn tasks(inputs: &[Input], chunk: usize) -> Vec<Task> {
    let mut out = Vec::new();
    for (file, input) in inputs.iter().enumerate() {
        let mut from = 0;
        while from < input.cut {
            let to = (from + chunk).min(input.cut);
            out.push(Task { file, from, to, tail: false });
            from = to;
        }
        if input.tail_end > 1 {
            out.push(Task { file, from: 1, to: input.tail_end, tail: true });
        }
    }
    // Largest first, so the threads finish together.
    out.sort_by_key(|task| std::cmp::Reverse(task.to - task.from));
    out
}

const BLOCK: usize = 1 << 12;

/// The separators of one block.
pub struct Index {
    /// A 0, then each separator's offset in the block plus 1: where the field after it starts.
    seps: Vec<u32>,
    /// One bit per separator, set for a newline.
    newlines: Vec<u64>,
    /// One bit per separator, set when a quote follows it.
    opens: Vec<u64>,
    /// One bit per separator, set when a quote precedes it.
    closes: Vec<u64>,
    /// Offsets plus 1 of quotes that neither follow nor precede a separator,
    /// and of quotes that are a whole field.
    bad: Vec<u32>,
}

impl Index {
    pub fn new() -> Self {
        Index {
            seps: vec![0; BLOCK + 96],
            newlines: vec![0; BLOCK / 64 + 4],
            opens: vec![0; BLOCK / 64 + 4],
            closes: vec![0; BLOCK / 64 + 4],
            bad: Vec::new(),
        }
    }

    /// Indexes `len` bytes at `block`, which start a line, and returns the number of separators.
    #[inline(always)]
    unsafe fn build(&mut self, block: *const u8, len: usize) -> usize {
        let newline = _mm256_set1_epi8(b'\n' as i8);
        let comma = _mm256_set1_epi8(b',' as i8);
        let quote = _mm256_set1_epi8(b'"' as i8);
        self.bad.clear();
        let seps = self.seps.as_mut_ptr().add(1);
        let (words, open_words, close_words) =
            (self.newlines.as_mut_ptr(), self.opens.as_mut_ptr(), self.closes.as_mut_ptr());
        let mut count = 0usize;
        let (mut word, mut filled) = (0usize, 0u32);
        let (mut bits, mut open_bits, mut close_bits) = (0u64, 0u64, 0u64);
        // Whether the byte before this word is a separator or a quote, and
        // whether it is a quote that only a separator after it can excuse, or
        // one that a separator after it makes a whole field.
        let (mut after_separator, mut after_quote, mut pending, mut pending_whole) = (1u64, 0u64, 0u64, 0u64);
        let mut at = 0;
        while at < len {
            let low = _mm256_loadu_si256(block.add(at) as *const __m256i);
            let high = _mm256_loadu_si256(block.add(at + 32) as *const __m256i);
            let mask = |needle: __m256i| {
                u64::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8(low, needle)) as u32)
                    | u64::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8(high, needle)) as u32) << 32
            };
            let mut newlines = mask(newline);
            let mut separators = newlines | mask(comma);
            let mut quotes = mask(quote);
            let mut next_quote = u64::from(*block.add(at + 64) == b'"') << 63;
            if len - at < 64 {
                let keep = (1u64 << (len - at)) - 1;
                newlines &= keep;
                separators &= keep;
                quotes &= keep;
                next_quote = 0;
            }
            let follows = quotes & (separators << 1 | after_separator);
            let lone = quotes & !follows & !(separators >> 1);
            let whole = follows & (separators >> 1);
            let late = (pending | pending_whole) & (pending_whole ^ !separators) & 1;
            if (lone | whole) & !(1 << 63) | late != 0 {
                if late != 0 {
                    self.bad.push(at as u32);
                }
                let mut rest = (lone | whole) & !(1 << 63);
                while rest != 0 {
                    self.bad.push((at + rest.trailing_zeros() as usize) as u32 + 1);
                    rest &= rest - 1;
                }
            }
            pending = lone >> 63;
            pending_whole = follows >> 63;
            let open_flags = _pext_u64(quotes >> 1 | next_quote, separators);
            let close_flags = _pext_u64(quotes << 1 | after_quote, separators);
            after_separator = separators >> 63;
            after_quote = quotes >> 63;

            let found = separators.count_ones() as usize;
            let out = seps.add(count);
            let first = at as u32 + 1;
            let mut rest = separators;
            for slot in 0..8 {
                *out.add(slot) = first + rest.trailing_zeros();
                rest &= rest.wrapping_sub(1);
            }
            if found > 8 {
                for slot in 8..16 {
                    *out.add(slot) = first + rest.trailing_zeros();
                    rest &= rest.wrapping_sub(1);
                }
                let mut slot = 16;
                while rest != 0 {
                    *out.add(slot) = first + rest.trailing_zeros();
                    rest &= rest - 1;
                    slot += 1;
                }
            }
            count += found;

            let flags = _pext_u64(newlines, separators);
            bits |= flags << filled;
            open_bits |= open_flags << filled;
            close_bits |= close_flags << filled;
            if filled as usize + found >= 64 {
                *words.add(word) = bits;
                *open_words.add(word) = open_bits;
                *close_words.add(word) = close_bits;
                word += 1;
                bits = (flags >> 1) >> (63 - filled);
                open_bits = (open_flags >> 1) >> (63 - filled);
                close_bits = (close_flags >> 1) >> (63 - filled);
                filled = filled + found as u32 - 64;
            } else {
                filled += found as u32;
            }
            at += 64;
        }
        for (words, bits) in [(words, bits), (open_words, open_bits), (close_words, close_bits)] {
            *words.add(word) = bits;
            *words.add(word + 1) = 0;
            *words.add(word + 2) = 0;
        }
        count
    }

    /// The first newline among separators `from..count`.
    #[inline(always)]
    unsafe fn next_newline(&self, from: usize, count: usize) -> Option<usize> {
        let mut word = from >> 6;
        let mut bits = *self.newlines.get_unchecked(word) >> (from & 63) << (from & 63);
        loop {
            if bits != 0 {
                let at = word * 64 + bits.trailing_zeros() as usize;
                return (at < count).then_some(at);
            }
            word += 1;
            if word * 64 >= count {
                return None;
            }
            bits = *self.newlines.get_unchecked(word);
        }
    }
}

/// Runs `line(block, ends, quoted)` on every line of the task that has `N`
/// fields, each either wrapped in quotes or free of them, skipping the file's
/// header. Field `k` of the line is bytes `ends[k]..ends[k + 1] - 1` of
/// `block`, quotes included, and bit `k` of `quoted` says whether it has them.
#[inline(always)]
pub unsafe fn run<const N: usize>(
    input: &Input,
    task: Task,
    index: &mut Index,
    mut line: impl FnMut(*const u8, *const u32, u64),
) {
    let (base, mut from, to) = if task.tail {
        (input.tail.as_ptr(), task.from, task.to)
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
        let (from, to) = (adjust(task.from), adjust(task.to));
        // Map the chunk's pages in one call instead of a fault every few pages.
        let first = from & !4095;
        libc::madvise(input.base.add(first) as *mut libc::c_void, to - first, libc::MADV_POPULATE_READ);
        (input.base, from, to)
    };
    if (task.tail && input.cut == 0) || (!task.tail && from == 0) {
        while *base.add(from) != b'\n' {
            from += 1;
        }
        from += 1;
    }
    let pattern = 1u64 << (N - 1);
    let window = (1u64 << N) - 1;
    while from < to {
        let len = BLOCK.min(to - from);
        let block = base.add(from);
        let count = index.build(block, len);
        let seps = index.seps.as_ptr();
        let (words, open_words, close_words) = (index.newlines.as_ptr(), index.opens.as_ptr(), index.closes.as_ptr());
        let first_quote = u64::from(*block == b'"');
        let window_at = |words: *const u64, at: usize| {
            let (word, shift) = (at >> 6, at & 63);
            *words.add(word) >> shift | (*words.add(word + 1) << 1) << (63 - shift)
        };
        let mut bad = 0;
        let mut at = 0;
        loop {
            let bits = window_at(words, at);
            let end;
            if bits & window == pattern {
                // A field opens with the quote after the separator before it.
                let opens = if at == 0 { window_at(open_words, 0) << 1 | first_quote } else { window_at(open_words, at - 1) };
                let closes = window_at(close_words, at);
                let ends = seps.add(at);
                end = *ends.add(N);
                at += N;
                if (opens ^ closes) & window == 0 && (bad == index.bad.len() || *index.bad.get_unchecked(bad) >= end) {
                    line(block, ends, opens);
                    continue;
                }
            } else {
                let Some(last) = index.next_newline(at, count) else { break };
                end = *seps.add(last + 1);
                at = last + 1;
            }
            while bad < index.bad.len() && *index.bad.get_unchecked(bad) < end {
                bad += 1;
            }
        }
        let consumed = *seps.add(at) as usize;
        if consumed == 0 {
            // A line longer than a block: leave it to the slow path.
            crate::bail();
            return;
        }
        from += consumed;
    }
}
