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

/// Masks that keep the first `len` bytes of a vector.
static KEEP: [[u8; 16]; 11] = {
    let mut table = [[0u8; 16]; 11];
    let mut len = 0;
    while len <= 10 {
        let mut at = 0;
        while at < len {
            table[len][at] = 0xFF;
            at += 1;
        }
        len += 1;
    }
    table
};

/// The key of a non-empty ID.
///
/// # Safety
/// `at..at + max(len, 16)` must be readable.
#[inline(always)]
pub unsafe fn pack(at: *const u8, len: usize) -> u64 {
    use std::arch::x86_64::*;
    if len <= 10 {
        let keep = _mm_loadu_si128(KEEP.get_unchecked(len).as_ptr() as *const __m128i);
        let bytes = _mm_and_si128(_mm_loadu_si128(at as *const __m128i), keep);
        let between = |low: u8, high: u8| {
            _mm_and_si128(
                _mm_cmpgt_epi8(bytes, _mm_set1_epi8(low as i8 - 1)),
                _mm_cmpgt_epi8(_mm_set1_epi8(high as i8 + 1), bytes),
            )
        };
        let (digit, upper, lower) = (between(b'0', b'9'), between(b'A', b'Z'), between(b'a', b'z'));
        // Each class maps to its codes by subtracting a constant.
        let offset = _mm_or_si128(
            _mm_or_si128(_mm_and_si128(digit, _mm_set1_epi8(0x2F)), _mm_and_si128(upper, _mm_set1_epi8(0x36))),
            _mm_and_si128(lower, _mm_set1_epi8(0x3C)),
        );
        let known = _mm_or_si128(_mm_or_si128(digit, upper), lower);
        if _mm_movemask_epi8(_mm_xor_si128(known, keep)) == 0 {
            let codes = _mm_sub_epi8(bytes, offset);
            let pairs = _mm_maddubs_epi16(codes, _mm_set1_epi16(0x0140));
            let fours = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_1000));
            let low = _mm_cvtsi128_si64(fours) as u64;
            let high = _mm_extract_epi32(fours, 2) as u32 as u64;
            return ((low & 0xFFFF_FFFF) << 36 | (low >> 32) << 12 | high >> 12) << 4;
        }
    }
    intern(std::slice::from_raw_parts(at, len))
}

/// `pack` for a slice, which need not have 16 readable bytes.
pub fn pack_slice(id: &[u8]) -> u64 {
    let mut padded = [0u8; 16];
    if id.len() <= 10 {
        padded[..id.len()].copy_from_slice(id);
        unsafe { pack(padded.as_ptr(), id.len()) }
    } else {
        intern(id)
    }
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
