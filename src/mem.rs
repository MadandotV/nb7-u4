//! Memory for the partitioned record streams: zeroed mappings, a bump arena
//! per thread, and a scatter that fills each partition a cache line at a time.

use std::arch::x86_64::*;

/// Zeroed, page-aligned memory that is never freed. `populate` maps it all
/// now, in one call, instead of a fault per page.
pub fn zeroed(bytes: usize, populate: bool) -> *mut u8 {
    let bytes = bytes.max(4096);
    let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE;
    unsafe {
        let at = libc::mmap(
            std::ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            if populate { flags | libc::MAP_POPULATE } else { flags },
            -1,
            0,
        );
        assert!(at != libc::MAP_FAILED, "mmap failed");
        at as *mut u8
    }
}

/// A thread's bump allocator. It reserves address space once, so that
/// growing it never takes the process-wide lock that `mmap` does, and maps
/// the pages a step at a time just ahead of their use.
pub struct Arena {
    at: *mut u8,
    /// Pages before this are mapped.
    mapped: *mut u8,
    end: *mut u8,
    regions: Vec<(*mut u8, usize)>,
    reserve: usize,
}

const STEP: usize = 2 << 20;

impl Arena {
    /// `reserve` is how much address space to set aside at a time.
    pub fn leak(reserve: usize) -> *mut Arena {
        Box::leak(Box::new(Arena {
            at: std::ptr::null_mut(),
            mapped: std::ptr::null_mut(),
            end: std::ptr::null_mut(),
            regions: Vec::new(),
            reserve: reserve.max(STEP).next_multiple_of(STEP),
        }))
    }

    /// Gives the memory back. Nothing may use it afterwards.
    pub unsafe fn release(&mut self) {
        for &(at, _) in &self.regions {
            let used = if at == self.regions.last().unwrap().0 { self.mapped as usize - at as usize } else { self.reserve };
            libc::madvise(at as *mut libc::c_void, used, libc::MADV_DONTNEED);
        }
    }

    /// `bytes` of zeroed memory aligned to 64 bytes.
    #[inline]
    pub unsafe fn alloc(&mut self, bytes: usize) -> *mut u8 {
        let bytes = (bytes + 63) & !63;
        if (self.end as usize) - (self.at as usize) < bytes {
            self.reserve = self.reserve.max(bytes.next_multiple_of(STEP));
            self.at = zeroed(self.reserve, false);
            self.mapped = self.at;
            self.end = self.at.add(self.reserve);
            self.regions.push((self.at, self.reserve));
        }
        let out = self.at;
        self.at = self.at.add(bytes);
        while self.mapped < self.at {
            // Without this call the pages are mapped one fault at a time, which is slower but works.
            libc::madvise(self.mapped as *mut libc::c_void, STEP, libc::MADV_POPULATE_WRITE);
            self.mapped = self.mapped.add(STEP);
        }
        out
    }
}

struct Stream<T> {
    chunks: Vec<*mut T>,
    /// Records written to the chunks.
    len: usize,
    at: *mut T,
    end: *mut T,
}

/// Records partitioned into streams of fixed-size chunks. `K` records fill a
/// whole number of cache lines, and a partition's records are staged in a
/// buffer that size and written out with non-temporal stores.
pub struct Scatter<T: Copy, const K: usize> {
    arena: *mut Arena,
    buffers: *mut T,
    /// Records pushed to each partition.
    counts: Vec<u32>,
    streams: Vec<Stream<T>>,
}

impl<T: Copy, const K: usize> Scatter<T, K> {
    /// Records per chunk.
    pub const RECORDS: usize = (1 << 14) / (K * std::mem::size_of::<T>()) * K;
    const BYTES: usize = K * std::mem::size_of::<T>();

    pub unsafe fn new(arena: *mut Arena, parts: usize) -> Self {
        assert!(Self::BYTES % 64 == 0);
        Scatter {
            arena,
            buffers: (*arena).alloc(parts * Self::BYTES) as *mut T,
            counts: vec![0; parts],
            streams: (0..parts)
                .map(|_| Stream { chunks: Vec::new(), len: 0, at: std::ptr::null_mut(), end: std::ptr::null_mut() })
                .collect(),
        }
    }

    /// Appends to a partition and returns the record's index in it.
    #[inline(always)]
    pub unsafe fn push(&mut self, part: usize, value: T) -> usize {
        let count = self.counts.get_unchecked_mut(part);
        let index = *count as usize;
        *count += 1;
        self.buffers.add(part * K + index % K).write(value);
        if index % K == K - 1 {
            self.flush(part);
        }
        index
    }

    #[inline(always)]
    unsafe fn room(&mut self, part: usize) {
        let stream = self.streams.get_unchecked_mut(part);
        if stream.at == stream.end {
            let chunk = (*self.arena).alloc(Self::RECORDS * std::mem::size_of::<T>()) as *mut T;
            stream.chunks.push(chunk);
            stream.at = chunk;
            stream.end = chunk.add(Self::RECORDS);
        }
    }

    #[inline(always)]
    unsafe fn flush(&mut self, part: usize) {
        self.room(part);
        let stream = self.streams.get_unchecked_mut(part);
        let from = self.buffers.add(part * K) as *const __m256i;
        let to = stream.at as *mut __m256i;
        for word in 0..Self::BYTES / 32 {
            _mm256_stream_si256(to.add(word), _mm256_load_si256(from.add(word)));
        }
        stream.at = stream.at.add(K);
        stream.len += K;
    }

    /// Writes out the partly filled buffers. Call once, after the last push.
    pub unsafe fn finish(&mut self) {
        for part in 0..self.counts.len() {
            let fill = self.counts[part] as usize % K;
            if fill > 0 {
                self.room(part);
                let stream = &mut self.streams[part];
                std::ptr::copy_nonoverlapping(self.buffers.add(part * K), stream.at, fill);
                stream.len += fill;
            }
        }
        _mm_sfence();
    }

    pub fn arena(&self) -> *mut Arena {
        self.arena
    }

    pub fn len(&self, part: usize) -> usize {
        self.streams[part].len
    }

    #[inline(always)]
    pub unsafe fn for_each_chunk(&self, part: usize, mut each: impl FnMut(&[T])) {
        let stream = self.streams.get_unchecked(part);
        let mut left = stream.len;
        for &chunk in &stream.chunks {
            let count = left.min(Self::RECORDS);
            each(std::slice::from_raw_parts(chunk, count));
            left -= count;
        }
    }

    /// Record `index` of a partition.
    #[inline(always)]
    pub unsafe fn at(&self, part: usize, index: usize) -> *mut T {
        self.streams.get_unchecked(part).chunks.get_unchecked(index / Self::RECORDS).add(index % Self::RECORDS)
    }
}
