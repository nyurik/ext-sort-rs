//! External sort of `(key, bytes)` records.
//!
//! [`ExternalSorter`](crate::ExternalSorter) sorts owned items, so every item is a separate value that is
//! moved around while sorting and allocated again when it is read back. [`RecordSorter`] is a lower level
//! alternative for data that is, or can be encoded as, a fixed-size [`RadixKey`] plus a byte record:
//!
//! * records are copied into one contiguous buffer, and only a small `(key, offset, length)` index is
//!   sorted, with a stable radix sort;
//! * the memory limit counts bytes, records plus index;
//! * runs are written as `key | varint length | record` and read back into a reused buffer, and the merger
//!   lends each record instead of allocating it;
//! * several producers can fill their own [`RecordBuffer`]s in parallel;
//! * at most `max_fan_in` runs are merged at once, with intermediate merges if there are more.
//!
//! # Example
//!
//! ```
//! use ext_sort::record::RecordSorterBuilder;
//!
//! let sorter = RecordSorterBuilder::new().with_buffer_bytes(1 << 20).build::<u64>().unwrap();
//! let mut buffer = sorter.buffer();
//! for (key, name) in [(3, "three"), (1, "one"), (2, "two")] {
//!     buffer.push(key, name.as_bytes()).unwrap();
//! }
//! buffer.finish().unwrap();
//!
//! let mut merger = sorter.merge().unwrap();
//! let mut sorted = Vec::new();
//! while let Some((key, record)) = merger.next().unwrap() {
//!     sorted.push((key, String::from_utf8(record.to_vec()).unwrap()));
//! }
//! assert_eq!(sorted, [(1, "one".into()), (2, "two".into()), (3, "three".into())]);
//! ```

use std::fs::File;
use std::io::{self, prelude::*, BufReader, BufWriter, SeekFrom};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

use rayon::prelude::*;

use crate::merger::LoserTree;
use crate::radix::RadixKey;

/// [`RecordSorter`] builder.
#[derive(Clone, Debug)]
pub struct RecordSorterBuilder {
    tmp_dir: Option<PathBuf>,
    buffer_bytes: usize,
    max_fan_in: usize,
    rw_buf_size: usize,
}

impl Default for RecordSorterBuilder {
    fn default() -> Self {
        RecordSorterBuilder {
            tmp_dir: None,
            buffer_bytes: 256 << 20,
            max_fan_in: 256,
            rw_buf_size: 256 << 10,
        }
    }
}

impl RecordSorterBuilder {
    /// Creates a builder with default parameters: 256 MiB buffers, a fan-in of 256 and 256 KiB file
    /// buffers, using the OS temporary directory.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the directory to create the temporary directory for runs in.
    pub fn with_tmp_dir(mut self, path: &Path) -> Self {
        self.tmp_dir = Some(path.into());
        self
    }

    /// Sets the bytes one [`RecordBuffer`] holds, records plus index, before it writes a sorted run.
    /// At most 4 GiB, so record offsets fit in 32 bits.
    pub fn with_buffer_bytes(mut self, buffer_bytes: usize) -> Self {
        self.buffer_bytes = buffer_bytes;
        self
    }

    /// Sets the maximum number of runs merged at once, at least 2. Each open run holds a file and a read
    /// buffer; beyond that, groups of runs are first merged into intermediate runs.
    pub fn with_max_fan_in(mut self, max_fan_in: usize) -> Self {
        self.max_fan_in = max_fan_in;
        self
    }

    /// Sets the read and write buffer size of each run file.
    pub fn with_rw_buf_size(mut self, rw_buf_size: usize) -> Self {
        self.rw_buf_size = rw_buf_size;
        self
    }

    /// Builds a [`RecordSorter`] for keys of type `K`.
    pub fn build<K: RadixKey>(self) -> io::Result<RecordSorter<K>> {
        let invalid = |msg| Err(io::Error::new(io::ErrorKind::InvalidInput, msg));
        if self.max_fan_in < 2 {
            return invalid("max fan-in must be at least 2");
        }
        if u32::try_from(self.buffer_bytes).is_err() {
            return invalid("buffer must be at most 4 GiB");
        }
        let tmp_dir = match &self.tmp_dir {
            Some(path) => tempfile::tempdir_in(path)?,
            None => tempfile::tempdir()?,
        };
        log::info!("using {} as a temporary directory", tmp_dir.path().display());
        Ok(RecordSorter {
            config: self,
            tmp_dir,
            runs: Mutex::default(),
            next_run: AtomicUsize::new(0),
            error: Mutex::default(),
            key_type: PhantomData,
        })
    }
}

/// External sorter of `(key, record)` pairs, see the [module documentation](self).
///
/// Records with equal keys come out in the order they were pushed into the same [`RecordBuffer`]. Across
/// buffers filled concurrently, equal keys come out in the order the buffers wrote their runs, which
/// depends on timing; make keys unique (e.g. include a producer id) when that matters.
pub struct RecordSorter<K> {
    config: RecordSorterBuilder,
    tmp_dir: tempfile::TempDir,
    runs: Mutex<Vec<Run>>,
    /// Runs are numbered in creation order, which breaks key ties.
    next_run: AtomicUsize,
    /// First error of a buffer that could only write its last run when dropped.
    error: Mutex<Option<io::Error>>,
    key_type: PhantomData<fn() -> K>,
}

/// A sorted run file.
struct Run {
    index: usize,
    file: File,
    records: u64,
}

impl<K: RadixKey> RecordSorter<K> {
    /// Creates a buffer to push records into. Buffers can be filled from different threads.
    pub fn buffer(&self) -> RecordBuffer<'_, K> {
        RecordBuffer {
            sorter: self,
            data: Vec::new(),
            entries: Vec::new(),
            scratch: Vec::new(),
        }
    }

    /// Merges the runs of all buffers, which must have been finished or dropped, into a sorted stream.
    pub fn merge(mut self) -> io::Result<RecordMerger<K>> {
        if let Some(err) = lock(&self.error).take() {
            return Err(err);
        }
        let fan_in = self.config.max_fan_in;
        let mut runs = std::mem::take(self.runs.get_mut().unwrap_or_else(PoisonError::into_inner));
        runs.sort_unstable_by_key(|run| run.index);
        while runs.len() > fan_in {
            log::debug!("merging {} runs in groups of {}", runs.len(), fan_in);
            let mut groups = Vec::with_capacity(runs.len().div_ceil(fan_in));
            let mut rest = runs.into_iter();
            while rest.len() > 0 {
                groups.push(rest.by_ref().take(fan_in).collect::<Vec<_>>());
            }
            // A group spans consecutive runs and keeps the index of its first one, so ties still resolve
            // in creation order across groups.
            runs = groups
                .into_par_iter()
                .map(|group| {
                    let index = group[0].index;
                    let mut merger = RecordMerger::<K>::new(group, self.config.rw_buf_size, None)?;
                    let mut out = self.run_writer(index)?;
                    while let Some((key, record)) = merger.next()? {
                        out.write(key, record)?;
                    }
                    out.finish()
                })
                .collect::<io::Result<_>>()?;
        }
        RecordMerger::new(runs, self.config.rw_buf_size, Some(self.tmp_dir))
    }

    fn run_writer(&self, index: usize) -> io::Result<RunWriter<K>> {
        let file = tempfile::tempfile_in(self.tmp_dir.path())?;
        Ok(RunWriter {
            index,
            out: BufWriter::with_capacity(self.config.rw_buf_size, file),
            records: 0,
            frame: Vec::with_capacity(K::BYTES + MAX_VARINT_LEN),
            key_type: PhantomData,
        })
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // A panic elsewhere cannot leave the run list or the error slot half updated.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One producer's in-memory batch. Records stay where they were copied; only the index is sorted.
///
/// Dropping a buffer writes its remaining records too, but [`RecordBuffer::finish`] reports errors right
/// away instead of from [`RecordSorter::merge`].
pub struct RecordBuffer<'a, K: RadixKey> {
    sorter: &'a RecordSorter<K>,
    data: Vec<u8>,
    entries: Vec<Entry<K>>,
    scratch: Vec<Entry<K>>,
}

impl<K: RadixKey> RecordBuffer<'_, K> {
    /// Adds a record. When the buffer is full, it is sorted and written to a run file.
    pub fn push(&mut self, key: K, record: &[u8]) -> io::Result<()> {
        self.push_with(key, |data| data.extend_from_slice(record))
    }

    /// Adds a record that `encode` appends to the given buffer, so it does not need to be encoded
    /// separately first.
    pub fn push_with(&mut self, key: K, encode: impl FnOnce(&mut Vec<u8>)) -> io::Result<()> {
        let start = self.data.len();
        encode(&mut self.data);
        let too_large = |_| io::Error::new(io::ErrorKind::InvalidInput, "record does not fit in the buffer");
        let offset = u32::try_from(start).map_err(too_large)?;
        let len = u32::try_from(self.data.len() - start).map_err(too_large)?;
        self.entries.push(Entry { key, offset, len });
        // The index needs room for the radix sort scratch copy too.
        if self.data.len() + self.entries.len() * 2 * size_of::<Entry<K>>() >= self.sorter.config.buffer_bytes {
            self.spill()?;
        }
        Ok(())
    }

    /// Writes the remaining records.
    pub fn finish(mut self) -> io::Result<()> {
        self.spill()
    }

    fn spill(&mut self) -> io::Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        log::debug!("sorting and writing a run of {} records", self.entries.len());
        sort_entries(&mut self.entries, &mut self.scratch);
        let index = self.sorter.next_run.fetch_add(1, Ordering::Relaxed);
        let mut out = self.sorter.run_writer(index)?;
        for entry in &self.entries {
            let record = &self.data[entry.offset as usize..][..entry.len as usize];
            out.write(entry.key, record)?;
        }
        let run = out.finish()?;
        lock(&self.sorter.runs).push(run);
        self.data.clear();
        self.entries.clear();
        Ok(())
    }
}

impl<K: RadixKey> Drop for RecordBuffer<'_, K> {
    fn drop(&mut self) {
        if let Err(err) = self.spill() {
            lock(&self.sorter.error).get_or_insert(err);
        }
    }
}

/// Index entry of a buffered record. Packing to 8 bytes keeps a `u128` key entry at 24 bytes instead of
/// 32, a quarter less memory traffic for every radix pass; fields are only ever copied out, never borrowed.
#[repr(C, packed(8))]
struct Entry<K> {
    key: K,
    offset: u32,
    len: u32,
}

impl<K: Copy> Clone for Entry<K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: Copy> Copy for Entry<K> {}

/// Below this many entries a comparison sort beats setting up the digit histograms.
const RADIX_THRESHOLD: usize = 256;

/// Stable least significant digit first radix sort of the entries by key. One counting pass computes the
/// histograms of all digits, then each digit is one scatter pass. Digits that are the same for every entry
/// are skipped, which is common for keys that do not use their full range.
fn sort_entries<K: RadixKey>(entries: &mut Vec<Entry<K>>, scratch: &mut Vec<Entry<K>>) {
    let n = entries.len();
    if n < RADIX_THRESHOLD {
        entries.sort_by_key(|entry| entry.key);
        return;
    }
    let mut counts = vec![[0usize; 256]; K::BYTES];
    for entry in entries.iter() {
        let key = entry.key;
        for (digit, count) in counts.iter_mut().enumerate() {
            count[usize::from(key.digit(digit))] += 1;
        }
    }
    for (digit, count) in counts.iter().enumerate() {
        if count.contains(&n) {
            continue;
        }
        if scratch.len() != n {
            // `Entry` has no default value, so the scratch space is sized by copying.
            scratch.clear();
            scratch.extend_from_slice(entries);
        }
        let mut next = [0usize; 256];
        let mut sum = 0;
        for (slot, &c) in next.iter_mut().zip(count) {
            *slot = sum;
            sum += c;
        }
        for entry in entries.iter() {
            let key = entry.key;
            let slot = &mut next[usize::from(key.digit(digit))];
            scratch[*slot] = *entry;
            *slot += 1;
        }
        std::mem::swap(entries, scratch);
    }
}

const MAX_VARINT_LEN: usize = 10;

/// Writes a run: `key | varint length | record`, repeated.
struct RunWriter<K> {
    index: usize,
    out: BufWriter<File>,
    records: u64,
    frame: Vec<u8>,
    key_type: PhantomData<fn(K)>,
}

impl<K: RadixKey> RunWriter<K> {
    fn write(&mut self, key: K, record: &[u8]) -> io::Result<()> {
        self.frame.clear();
        key.write(&mut self.frame);
        let mut len = record.len() as u64;
        while len >= 0x80 {
            self.frame.push(len as u8 | 0x80);
            len >>= 7;
        }
        self.frame.push(len as u8);
        self.out.write_all(&self.frame)?;
        self.out.write_all(record)?;
        self.records += 1;
        Ok(())
    }

    fn finish(self) -> io::Result<Run> {
        let mut file = self.out.into_inner().map_err(io::IntoInnerError::into_error)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(Run {
            index: self.index,
            file,
            records: self.records,
        })
    }
}

/// Streams the records of all runs in key order, see [`RecordMerger::next`].
pub struct RecordMerger<K> {
    cursors: Vec<Cursor<K>>,
    tree: LoserTree,
    /// Cursor whose record was returned last. It advances on the next call, so the record can be lent.
    returned: Option<usize>,
    /// Keeps the run directory until the runs are read.
    _tmp_dir: Option<tempfile::TempDir>,
}

impl<K: RadixKey> RecordMerger<K> {
    fn new(runs: Vec<Run>, rw_buf_size: usize, tmp_dir: Option<tempfile::TempDir>) -> io::Result<Self> {
        let cursors = runs
            .into_iter()
            .map(|run| Cursor::new(run, rw_buf_size))
            .collect::<io::Result<Vec<_>>>()?;
        let tree = LoserTree::new(cursors.len(), |a, b| before(&cursors, a, b));
        Ok(RecordMerger {
            cursors,
            tree,
            returned: None,
            _tmp_dir: tmp_dir,
        })
    }

    /// Returns the next record in key order, or `None` at the end. The record is lent until the next call,
    /// which is why this is not an [`Iterator`].
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> io::Result<Option<(K, &[u8])>> {
        if let Some(last) = self.returned.take() {
            self.cursors[last].advance()?;
            let cursors = &self.cursors;
            self.tree.replay(last, |a, b| before(cursors, a, b));
        }
        let Some(winner) = self.tree.winner() else {
            return Ok(None);
        };
        let cursor = &self.cursors[winner];
        Ok(cursor.head.map(|key| {
            self.returned = Some(winner);
            (key, cursor.record())
        }))
    }
}

/// Exhausted cursors go last; ties go to the earlier run.
#[inline]
fn before<K: RadixKey>(cursors: &[Cursor<K>], a: usize, b: usize) -> bool {
    match (cursors[a].head, cursors[b].head) {
        (Some(x), Some(y)) => (x, a) < (y, b),
        (x, _) => x.is_some(),
    }
}

/// Reads the records of one run.
struct Cursor<K> {
    reader: BufReader<File>,
    remaining: u64,
    head: Option<K>,
    /// Length of the head record.
    len: usize,
    /// Whether the head record is lent straight from the read buffer, or was copied into `record` because
    /// it straddles the end of the buffer.
    in_buffer: bool,
    record: Vec<u8>,
}

impl<K: RadixKey> Cursor<K> {
    fn new(run: Run, rw_buf_size: usize) -> io::Result<Self> {
        let mut cursor = Cursor {
            reader: BufReader::with_capacity(rw_buf_size, run.file),
            remaining: run.records,
            head: None,
            len: 0,
            in_buffer: false,
            record: Vec::new(),
        };
        cursor.advance()?;
        Ok(cursor)
    }

    #[inline]
    fn record(&self) -> &[u8] {
        if self.in_buffer {
            &self.reader.buffer()[..self.len]
        } else {
            &self.record
        }
    }

    /// The record count is known, so any end of file while reading is a truncated run, not the end.
    fn advance(&mut self) -> io::Result<()> {
        if self.in_buffer {
            self.reader.consume(self.len);
            self.in_buffer = false;
        }
        if self.remaining == 0 {
            self.head = None;
            return Ok(());
        }
        self.remaining -= 1;

        let invalid = || io::Error::new(io::ErrorKind::InvalidData, "corrupt run file");
        if self.reader.buffer().is_empty() {
            self.reader.fill_buf()?;
        }
        let buf = self.reader.buffer();
        let (key, len) = if buf.len() >= K::BYTES + MAX_VARINT_LEN {
            // The whole header is in the buffer, so it is parsed in place without copying.
            let (key, rest) = buf.split_at(K::BYTES);
            let key = K::read(key).ok_or_else(invalid)?;
            let (len, varint_len) = decode_varint(rest).ok_or_else(invalid)?;
            self.reader.consume(K::BYTES + varint_len);
            (key, len)
        } else {
            self.record.resize(K::BYTES, 0);
            self.reader.read_exact(&mut self.record)?;
            let key = K::read(&self.record).ok_or_else(invalid)?;
            let mut len = 0;
            for shift in (0..64).step_by(7) {
                let mut byte = [0];
                self.reader.read_exact(&mut byte)?;
                len |= u64::from(byte[0] & 0x7f) << shift;
                if byte[0] < 0x80 {
                    break;
                }
            }
            (key, len)
        };
        self.len = usize::try_from(len).map_err(|_| invalid())?;

        if self.reader.buffer().is_empty() {
            self.reader.fill_buf()?;
        }
        if self.reader.buffer().len() >= self.len {
            self.in_buffer = true;
        } else {
            self.record.resize(self.len, 0);
            self.reader.read_exact(&mut self.record)?;
        }
        self.head = Some(key);
        Ok(())
    }
}

/// Decodes a LEB128 varint from the start of `buf`, returning it and its length in bytes.
#[inline]
fn decode_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0;
    for (i, &byte) in buf.iter().take(MAX_VARINT_LEN).enumerate() {
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte < 0x80 {
            return Some((value, i + 1));
        }
    }
    None
}

#[cfg(test)]
mod test {
    use rand::Rng;
    use rstest::*;

    use super::{sort_entries, Entry, RecordMerger, RecordSorter, RecordSorterBuilder};

    fn sorter(buffer_bytes: usize, max_fan_in: usize) -> RecordSorter<u16> {
        RecordSorterBuilder::new()
            .with_tmp_dir(std::path::Path::new("./"))
            .with_buffer_bytes(buffer_bytes)
            .with_max_fan_in(max_fan_in)
            .with_rw_buf_size(64)
            .build()
            .unwrap()
    }

    fn drain(mut merger: RecordMerger<u16>) -> Vec<(u16, Vec<u8>)> {
        let mut out = Vec::new();
        while let Some((key, record)) = merger.next().unwrap() {
            out.push((key, record.to_vec()));
        }
        out
    }

    /// Few distinct keys, and records holding the push position, so equal keys check stability.
    fn records(n: usize) -> Vec<(u16, Vec<u8>)> {
        let mut rng = rand::thread_rng();
        (0..n)
            .map(|i| (rng.gen_range(0..50), format!("{i}").into_bytes()))
            .collect()
    }

    #[rstest]
    // one run
    #[case(1 << 30, 64)]
    // many runs, merged at once
    #[case(2000, 64)]
    // many runs, with one and with several intermediate merge passes
    #[case(2000, 3)]
    #[case(300, 2)]
    fn test_equals_stable_sort(#[case] buffer_bytes: usize, #[case] max_fan_in: usize) {
        let input = records(3000);
        let sorter = sorter(buffer_bytes, max_fan_in);
        let mut buffer = sorter.buffer();
        for (key, record) in &input {
            buffer.push(*key, record).unwrap();
        }
        buffer.finish().unwrap();
        let mut expected = input;
        expected.sort_by_key(|(key, _)| *key);
        assert_eq!(drain(sorter.merge().unwrap()), expected);
    }

    #[test]
    fn test_multiple_producers() {
        let sorter = sorter(500, 4);
        // Unique keys per producer, as the sorter documents for deterministic output.
        let inputs: Vec<Vec<(u16, Vec<u8>)>> = (0..4u16)
            .map(|p| records(800).into_iter().map(|(k, r)| (k * 4 + p, r)).collect())
            .collect();
        std::thread::scope(|scope| {
            for input in &inputs {
                let mut buffer = sorter.buffer();
                scope.spawn(move || {
                    for (key, record) in input {
                        buffer.push_with(*key, |data| data.extend_from_slice(record)).unwrap();
                    }
                    // dropping the buffer writes its last run
                });
            }
        });
        let mut expected = inputs.concat();
        expected.sort_by_key(|(key, _)| *key);
        assert_eq!(drain(sorter.merge().unwrap()), expected);
    }

    #[test]
    fn test_empty_input() {
        let empty = sorter(1000, 2);
        empty.buffer().finish().unwrap();
        assert_eq!(drain(empty.merge().unwrap()), Vec::new());

        let sorter = sorter(1000, 2);
        let mut buffer = sorter.buffer();
        buffer.push(1, b"").unwrap();
        buffer.push(0, &[7; 300]).unwrap();
        drop(buffer);
        assert_eq!(drain(sorter.merge().unwrap()), vec![(0, vec![7; 300]), (1, vec![])]);
    }

    #[test]
    fn test_rejects_invalid_config() {
        assert!(RecordSorterBuilder::new().with_max_fan_in(1).build::<u8>().is_err());
        assert!(RecordSorterBuilder::new()
            .with_buffer_bytes(usize::MAX)
            .build::<u8>()
            .is_err());
    }

    #[test]
    fn test_sort_entries() {
        let mut rng = rand::thread_rng();
        for n in [0, 1, 255, 256, 5000] {
            for spread in [1u128, 1 << 8, 1 << 40, u128::MAX] {
                // random keys, then keys whose low digits are a sequence, with ties, as the sort skips them
                for variant in 0..3 {
                    let mut entries: Vec<Entry<u128>> = (0..n)
                        .map(|i| {
                            let high = rng.gen_range(0..spread);
                            let key = match variant {
                                0 => high << 16,
                                1 => high << 32 | u128::from(i),
                                _ => high << 32 | u128::from(i / 7),
                            };
                            Entry { key, offset: i, len: 0 }
                        })
                        .collect();
                    let mut expected: Vec<(u128, u32)> = entries.iter().map(|e| (e.key, e.offset)).collect();
                    expected.sort_by_key(|e| e.0);
                    sort_entries(&mut entries, &mut Vec::new());
                    let actual: Vec<(u128, u32)> = entries.iter().map(|e| (e.key, e.offset)).collect();
                    assert_eq!(actual, expected, "n={n} spread={spread} variant={variant}");
                }
            }
        }
    }
}
