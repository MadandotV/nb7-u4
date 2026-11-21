//! A minimal Parquet writer: one row group, one uncompressed PLAIN data page
//! (v1) per column. Callers hand in values already PLAIN-encoded.

use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    /// BYTE_ARRAY annotated as a UTF-8 string.
    Str,
    /// INT64.
    I64,
    /// INT32.
    I32,
    /// INT32 annotated as DATE (days since the epoch).
    Date,
}

pub struct Spec {
    pub name: &'static str,
    pub kind: Kind,
    pub optional: bool,
}

#[derive(Default)]
pub struct Column {
    /// PLAIN-encoded non-null values.
    pub values: Vec<u8>,
    /// For an optional column: one bit per row, least significant bit first,
    /// set when the row has a value.
    pub present: Vec<u8>,
}

impl Column {
    #[inline]
    pub fn push_str(&mut self, value: &[u8]) {
        self.values.extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.values.extend_from_slice(value);
    }
    #[inline]
    pub fn push_i64(&mut self, value: i64) {
        self.values.extend_from_slice(&value.to_le_bytes());
    }
    #[inline]
    pub fn push_i32(&mut self, value: i32) {
        self.values.extend_from_slice(&value.to_le_bytes());
    }
    /// Marks row `row` of an optional column as present or null.
    #[inline]
    pub fn push_present(&mut self, row: usize, present: bool) {
        if row % 8 == 0 {
            self.present.push(0);
        }
        if present {
            *self.present.last_mut().unwrap() |= 1 << (row % 8);
        }
    }
}

struct Thrift {
    out: Vec<u8>,
    last: Vec<i16>,
}

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

impl Thrift {
    fn new() -> Self {
        Self { out: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.out.push(value as u8 | 0x80);
            value >>= 7;
        }
        self.out.push(value as u8);
    }
    fn zigzag(&mut self, value: i64) {
        self.varint(((value << 1) ^ (value >> 63)) as u64);
    }
    fn field(&mut self, id: i16, kind: u8) {
        let last = self.last.last_mut().unwrap();
        let delta = id - *last;
        *last = id;
        if (1..=15).contains(&delta) {
            self.out.push((delta as u8) << 4 | kind);
        } else {
            self.out.push(kind);
            self.zigzag(i64::from(id));
        }
    }
    fn i32(&mut self, id: i16, value: i32) {
        self.field(id, T_I32);
        self.zigzag(i64::from(value));
    }
    fn i64(&mut self, id: i16, value: i64) {
        self.field(id, T_I64);
        self.zigzag(value);
    }
    fn binary(&mut self, id: i16, value: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(value.len() as u64);
        self.out.extend_from_slice(value);
    }
    fn list(&mut self, id: i16, kind: u8, size: usize) {
        self.field(id, T_LIST);
        self.list_header(kind, size);
    }
    fn list_header(&mut self, kind: u8, size: usize) {
        if size < 15 {
            self.out.push((size as u8) << 4 | kind);
        } else {
            self.out.push(0xF0 | kind);
            self.varint(size as u64);
        }
    }
    /// Opens a struct that is the value of field `id`.
    fn open(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }
    /// Opens a struct that is a list element.
    fn open_element(&mut self) {
        self.last.push(0);
    }
    fn close(&mut self) {
        self.out.push(0);
        self.last.pop();
    }
}

fn physical(kind: Kind) -> i32 {
    match kind {
        Kind::Date | Kind::I32 => 1,
        Kind::I64 => 2,
        Kind::Str => 6,
    }
}

/// Writes one Parquet file holding `rows` rows.
pub fn write(path: &Path, specs: &[Spec], columns: &[Column], rows: usize) -> std::io::Result<()> {
    let mut file = File::create(path)?;
    let mut offset: u64 = 4;
    let mut head = Vec::with_capacity(4096);
    head.extend_from_slice(b"PAR1");
    // (data page offset, total size, levels) per column.
    let mut chunks: Vec<(u64, u64)> = Vec::new();
    let mut pending: Vec<u8> = head;
    if rows > 0 {
        for (spec, column) in specs.iter().zip(columns) {
            let mut levels = Vec::new();
            if spec.optional {
                // RLE/bit-packed hybrid, bit width 1: one bit-packed run.
                let groups = rows.div_ceil(8);
                let mut run = Thrift::new();
                run.varint((groups as u64) << 1 | 1);
                levels.extend_from_slice(&((run.out.len() + groups) as u32).to_le_bytes());
                levels.extend_from_slice(&run.out);
                levels.extend_from_slice(&column.present[..groups]);
            }
            let size = levels.len() + column.values.len();
            assert!(size < i32::MAX as usize, "page too large");
            let mut header = Thrift::new();
            header.i32(1, 0); // DATA_PAGE
            header.i32(2, size as i32);
            header.i32(3, size as i32);
            header.open(5);
            header.i32(1, rows as i32);
            header.i32(2, 0); // PLAIN
            header.i32(3, 3); // RLE
            header.i32(4, 3); // RLE
            header.close();
            header.out.push(0);
            pending.extend_from_slice(&header.out);
            pending.extend_from_slice(&levels);
            let total = (header.out.len() + size) as u64;
            chunks.push((offset, total));
            offset += total;
            if column.values.len() >= 1 << 16 {
                file.write_all(&pending)?;
                pending.clear();
                file.write_all(&column.values)?;
            } else {
                pending.extend_from_slice(&column.values);
            }
        }
    }

    let mut meta = Thrift::new();
    meta.i32(1, 1);
    meta.list(2, T_STRUCT, specs.len() + 1);
    meta.open_element();
    meta.binary(4, b"schema");
    meta.i32(5, specs.len() as i32);
    meta.close();
    for spec in specs {
        meta.open_element();
        meta.i32(1, physical(spec.kind));
        meta.i32(3, i32::from(spec.optional));
        meta.binary(4, spec.name.as_bytes());
        match spec.kind {
            Kind::Str => {
                meta.i32(6, 0); // UTF8
                meta.open(10);
                meta.open(1);
                meta.close();
                meta.close();
            }
            Kind::Date => {
                meta.i32(6, 6); // DATE
                meta.open(10);
                meta.open(6);
                meta.close();
                meta.close();
            }
            Kind::I64 | Kind::I32 => {}
        }
        meta.close();
    }
    meta.i64(3, rows as i64);
    if rows > 0 {
        meta.list(4, T_STRUCT, 1);
        meta.open_element();
        meta.list(1, T_STRUCT, specs.len());
        for (spec, &(at, total)) in specs.iter().zip(&chunks) {
            meta.open_element();
            meta.i64(2, at as i64);
            meta.open(3);
            meta.i32(1, physical(spec.kind));
            meta.list(2, T_I32, 2);
            meta.zigzag(0); // PLAIN
            meta.zigzag(3); // RLE
            meta.list(3, T_BINARY, 1);
            meta.varint(spec.name.len() as u64);
            meta.out.extend_from_slice(spec.name.as_bytes());
            meta.i32(4, 0); // UNCOMPRESSED
            meta.i64(5, rows as i64);
            meta.i64(6, total as i64);
            meta.i64(7, total as i64);
            meta.i64(9, at as i64);
            meta.close();
            meta.close();
        }
        meta.i64(2, (offset - 4) as i64);
        meta.i64(3, rows as i64);
        meta.close();
    } else {
        meta.list(4, T_STRUCT, 0);
    }
    meta.binary(6, b"shopsess");
    meta.out.push(0);
    pending.extend_from_slice(&meta.out);
    pending.extend_from_slice(&(meta.out.len() as u32).to_le_bytes());
    pending.extend_from_slice(b"PAR1");
    file.write_all(&pending)
}

pub const USER_DAILY: [Spec; 6] = [
    Spec { name: "user_id", kind: Kind::Str, optional: false },
    Spec { name: "day", kind: Kind::Date, optional: false },
    Spec { name: "sessions", kind: Kind::I64, optional: false },
    Spec { name: "events", kind: Kind::I64, optional: false },
    Spec { name: "revenue_cents", kind: Kind::I64, optional: false },
    Spec { name: "top_category", kind: Kind::Str, optional: true },
];

pub const CAMPAIGN_DAILY: [Spec; 7] = [
    Spec { name: "campaign_id", kind: Kind::Str, optional: false },
    Spec { name: "channel", kind: Kind::Str, optional: false },
    Spec { name: "day", kind: Kind::Date, optional: false },
    Spec { name: "sessions", kind: Kind::I64, optional: false },
    Spec { name: "purchasing_sessions", kind: Kind::I64, optional: false },
    Spec { name: "revenue_cents", kind: Kind::I64, optional: false },
    Spec { name: "assisted_sessions", kind: Kind::I64, optional: false },
];
