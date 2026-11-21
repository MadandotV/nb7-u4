//! IDs as exact 64-bit keys.
//!
//! An ID of 1 to 10 ASCII letters and digits is packed 6 bits a character,
//! left-aligned, with the low 4 bits zero; unsigned comparison of two such
//! keys is byte-wise comparison of the IDs. Any other ID is interned, and its
//! key is `index << 4 | 15`. No key is 0.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Mutex;

const fn codes() -> [u8; 256] {
    let mut table = [0u8; 256];
    let mut at = 0;
    while at < 10 {
        table[b'0' as usize + at] = 1 + at as u8;
        at += 1;
    }
    at = 0;
    while at < 26 {
        table[b'A' as usize + at] = 11 + at as u8;
        table[b'a' as usize + at] = 37 + at as u8;
        at += 1;
    }
    table
}

pub static CODE: [u8; 256] = codes();
const CHARS: &[u8; 63] = b"\x000123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

struct Interner {
    index: HashMap<Vec<u8>, u64>,
    values: Vec<Vec<u8>>,
}

static INTERNER: Mutex<Option<Interner>> = Mutex::new(None);

#[cold]
#[inline(never)]
fn intern(bytes: &[u8]) -> u64 {
    let mut guard = INTERNER.lock().unwrap();
    let interner = guard.get_or_insert_with(|| Interner { index: HashMap::new(), values: Vec::new() });
    if let Some(&at) = interner.index.get(bytes) {
        return at << 4 | 15;
    }
    let at = interner.values.len() as u64;
    interner.index.insert(bytes.to_vec(), at);
    interner.values.push(bytes.to_vec());
    at << 4 | 15
}

/// The key of a non-empty ID.
///
/// # Safety
/// `at..at + len` must be readable.
#[inline(always)]
pub unsafe fn pack(at: *const u8, len: usize) -> u64 {
    if len <= 10 {
        let mut key = 0u64;
        let mut ok = true;
        for offset in 0..len {
            let code = CODE[*at.add(offset) as usize];
            ok &= code != 0;
            key = key << 6 | u64::from(code);
        }
        if ok {
            return key << (6 * (10 - len) + 4);
        }
    }
    intern(std::slice::from_raw_parts(at, len))
}

/// Appends the ID of `key` to `out`.
#[inline]
pub fn unpack(key: u64, out: &mut Vec<u8>) {
    if key & 15 == 0 {
        let mut rest = key;
        while rest != 0 {
            out.push(CHARS[(rest >> 58) as usize]);
            rest <<= 6;
        }
    } else {
        let guard = INTERNER.lock().unwrap();
        out.extend_from_slice(&guard.as_ref().unwrap().values[(key >> 4) as usize]);
    }
}

#[cold]
#[inline(never)]
fn compare_slow(a: u64, b: u64) -> Ordering {
    let (mut left, mut right) = (Vec::new(), Vec::new());
    unpack(a, &mut left);
    unpack(b, &mut right);
    left.cmp(&right)
}

/// Byte-wise order of the two IDs.
#[inline(always)]
pub fn compare(a: u64, b: u64) -> Ordering {
    if (a | b) & 15 == 0 {
        a.cmp(&b)
    } else {
        compare_slow(a, b)
    }
}

#[inline(always)]
pub fn hash(key: u64) -> u64 {
    key.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Appends the ID of `key` as a Parquet PLAIN string: its length, then its bytes.
#[inline]
pub fn unpack_string(key: u64, out: &mut Vec<u8>) {
    let at = out.len();
    out.extend_from_slice(&[0; 4]);
    unpack(key, out);
    let len = (out.len() - at - 4) as u32;
    out[at..at + 4].copy_from_slice(&len.to_le_bytes());
}
