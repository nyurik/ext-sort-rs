//! Benchmark workload: `n` records of a `u128` key plus a 64-byte payload, sorted with a 256 MiB budget
//! on one thread. Each mode runs in its own process so `perf stat` attributes cycles to it:
//!
//! ```text
//! cargo build --release --example bench
//! perf stat -e cycles:u,instructions:u target/release/examples/bench <mode> 10000000
//! ```
//!
//! `gen` only generates the keys; subtract it from the other modes to get the sorting cost.
//! Set `BENCH_BUDGET_MIB` to change the memory budget, and with it the number of chunks to merge.
//! The keys are composite, with long shared prefixes and an increasing sequence number: each input row
//! yields one or two keys at each of 15 levels of a Hilbert-ordered quadtree, keyed by
//! `(curve position, category, row)`.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};

use ext_sort::{
    ExternalChunk, ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder, RawExternalChunk, RawItem,
    RmpExternalChunk,
};

const PAYLOAD: usize = 64;

/// Memory budget, 256 MiB unless overridden by `BENCH_BUDGET_MIB`; smaller budgets create more chunks.
fn budget() -> usize {
    std::env::var("BENCH_BUDGET_MIB").map_or(256, |mib| mib.parse().expect("budget in MiB")) << 20
}
const RW_BUF: usize = 256 << 10;

/// Position along the Hilbert curve of a `2^level` square grid, after all positions of the coarser levels.
fn hilbert_index(level: u8, mut x: u64, mut y: u64) -> u64 {
    let n = 1 << level;
    let mut d = 0;
    let mut s = n / 2;
    while s > 0 {
        let rx = u64::from(x & s != 0);
        let ry = u64::from(y & s != 0);
        d += s * s * ((3 * rx) ^ ry);
        if ry == 0 {
            if rx == 1 {
                x = n - 1 - x;
                y = n - 1 - y;
            }
            std::mem::swap(&mut x, &mut y);
        }
        s /= 2;
    }
    ((1 << (2 * level)) - 1) / 3 + d
}

fn workload(n: usize) -> Vec<u128> {
    let mut state = 0x1234_5678_u64;
    let mut rnd = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut out = Vec::with_capacity(n);
    let mut row = 0_u64;
    while out.len() < n {
        row += 1;
        let (x, y) = (rnd() % (1 << 14), rnd() % (1 << 14));
        let category = rnd() % 4;
        for level in (0..=14u8).rev() {
            let shift = 14 - level;
            for piece in 0..=rnd() % 2 {
                let px = ((x >> shift) + piece).min((1 << level) - 1);
                let position = hilbert_index(level, px, y >> shift);
                out.push(u128::from(position << 8 | category) << 64 | u128::from(row));
            }
        }
    }
    out.truncate(n);
    out
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct Item(u128, Vec<u8>);

/// Hand-written chunk format: `key | u32 len | bytes`.
struct RawChunk(io::Take<BufReader<File>>);

impl Iterator for RawChunk {
    type Item = io::Result<Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut key = [0; 16];
        match self.0.read_exact(&mut key) {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return None,
            Err(e) => return Some(Err(e)),
            Ok(()) => {}
        }
        let mut len = [0; 4];
        if let Err(e) = self.0.read_exact(&mut len) {
            return Some(Err(e));
        }
        let mut data = vec![0; u32::from_le_bytes(len) as usize];
        Some(
            self.0
                .read_exact(&mut data)
                .map(|()| Item(u128::from_le_bytes(key), data)),
        )
    }
}

impl ExternalChunk<Item> for RawChunk {
    type SerializationError = io::Error;
    type DeserializationError = io::Error;

    fn new(reader: io::Take<BufReader<File>>) -> Self {
        Self(reader)
    }

    fn dump(w: &mut BufWriter<File>, items: impl IntoIterator<Item = Item>) -> io::Result<()> {
        for Item(key, data) in items {
            w.write_all(&key.to_le_bytes())?;
            w.write_all(&(data.len() as u32).to_le_bytes())?;
            w.write_all(&data)?;
        }
        Ok(())
    }
}

impl RawItem for Item {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.0.to_le_bytes());
        buf.extend_from_slice(&self.1);
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        let (key, payload) = bytes.split_first_chunk().ok_or(io::ErrorKind::InvalidData)?;
        Ok(Item(u128::from_le_bytes(*key), payload.to_vec()))
    }
}

/// Checks the output order and folds it into a checksum that matches across modes.
#[derive(Default)]
struct Check {
    count: u64,
    sum: u64,
    last: u128,
}

impl Check {
    fn add(&mut self, key: u128, payload: &[u8]) {
        assert!(key >= self.last, "output is not sorted");
        self.last = key;
        self.count += 1;
        self.sum = self
            .sum
            .wrapping_add((key >> 72) as u64)
            .wrapping_add(payload.len() as u64);
    }
}

fn ext_sort<C: ExternalChunk<Item>>(keys: &[u128]) -> Check
where
    C::SerializationError: std::fmt::Debug,
    C::DeserializationError: std::fmt::Debug,
{
    let record = vec![7u8; PAYLOAD];
    // Size chunks to the memory budget: payload + key + `Vec` header per item.
    let items = budget() / (PAYLOAD + 16 + 24);
    let sorter: ExternalSorter<Item, io::Error, LimitedBufferBuilder, C> = ExternalSorterBuilder::new()
        .with_buffer(LimitedBufferBuilder::new(items, true))
        .with_threads_number(1)
        .with_rw_buf_size(RW_BUF)
        .build()
        .unwrap();
    let sorted = sorter
        .sort_by(keys.iter().map(|&k| Ok(Item(k, record.clone()))), |a, b| a.0.cmp(&b.0))
        .unwrap();
    let mut check = Check::default();
    for item in sorted {
        let item = item.unwrap();
        check.add(item.0, &item.1);
    }
    check
}

fn main() {
    let mode = std::env::args().nth(1).expect("usage: bench <mode> [n]");
    let n: usize = std::env::args().nth(2).map_or(10_000_000, |s| s.parse().unwrap());
    let keys = workload(n);
    let check = match mode.as_str() {
        "gen" => Check {
            count: keys.len() as u64,
            ..Check::default()
        },
        "ext-rmp" => ext_sort::<RmpExternalChunk<Item>>(&keys),
        "ext-raw" => ext_sort::<RawChunk>(&keys),
        "ext-rawchunk" => ext_sort::<RawExternalChunk<Item>>(&keys),
        _ => panic!("unknown mode {mode}"),
    };
    eprintln!("{mode}: {} records, checksum {}", check.count, check.sum);
}
