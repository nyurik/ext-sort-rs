# Performance proposal for `ext-sort`

This branch makes `ext-sort` faster in two ways. It speeds up the existing `ExternalSorter` without breaking its
API, and it adds an opt-in `record::RecordSorter` for data that is a fixed-size key plus a byte record. On the
benchmark below, `RecordSorter` is 13x faster than the default `ExternalSorter`. Each change is a separate
commit with its own measurements, so each can be taken or left on its own.

## Benchmark

`examples/bench.rs` sorts 10M records, each a `u128` key plus a 64-byte payload. The memory budget is 256 MiB
and everything runs on one thread. Each mode runs in its own process. The keys are composite, with long shared
prefixes and an increasing sequence number in the low bits. The bench checks that the output is sorted.

Cost is measured as user-space cycles, `perf stat -e cycles:u,instructions:u`, taking the minimum of 3 runs.
Generating the keys costs 0.70 Gcycles, and that is subtracted from every number below. The machine was shared,
so cycle counts vary by ±3–5% between runs. Instruction counts are stable and are shown as a cross-check. Wall
time and RSS are for context only: the runs are mostly bound by temp-file I/O to an NVMe disk.

| Mode (all measured the same day)                                   | Gcycles | Ginstr | wall  | max RSS |
|--------------------------------------------------------------------|--------:|-------:|------:|--------:|
| baseline `ExternalSorter` + `RmpExternalChunk` (default)           |   47.40 | 152.24 | 35.3s |  602 MB |
| baseline `ExternalSorter` + hand-written `key\|len\|bytes` chunk   |   12.99 |  21.95 | 13.4s |  602 MB |
| final `ExternalSorter` + `RmpExternalChunk`                        |   33.19 |  95.79 | 26.7s |  602 MB |
| final `ExternalSorter` + same hand-written chunk                   |   12.08 |  19.84 | 12.7s |  602 MB |
| final `ExternalSorter` + `RawExternalChunk`                        |   10.80 |  16.96 | 11.4s |  602 MB |
| final `RecordSorter`                                               |    3.64 |   6.88 |  5.0s |  421 MB |

## Changes (commit order)

1. **`chore: fix clippy warnings and apply rustfmt`**: no functional change. Adds a default
   `ChunkBuffer::is_empty`.
2. **`bench: add a reproducible external sort benchmark`**: `examples/bench.rs`. It produces the same keys and
   checksum as the original harness. `BENCH_BUDGET_MIB` and `BENCH_MAX_FAN_IN` change the chunk count and the
   fan-in.
3. **`perf(merger): loser tree`**: replaces `BinaryHeapMerger` with `LoserTreeMerger`.
   `BinaryHeapMerger` remains as a deprecated type alias. The heap needed two comparisons per level and moved
   whole items together with a copy of the comparator. The loser tree needs one comparison per level and only
   moves indexes. Ties still go to the earlier chunk, so the output stays stable.
   ext-raw: 13.76 → 13.46 Gcycles with 4 chunks; 11.88 → 11.17 with 125 chunks (8 MiB budget).
   This commit also fixes the error path: a read error no longer drops the item just taken, and a priming error
   no longer re-reads every chunk.
4. **`perf(chunk): ChunkReader`**: `io::Take` does not override `read_exact`. As a result, MessagePack's
   one-byte reads went through `default_read_exact`, `Take::read` and `BufReader::read`, which was 38% of
   cycles. `ChunkReader` (`From<io::Take<..>>`, so the trait is unchanged) serves `read_exact` from the buffer,
   inlined. ext-rmp: 47.65 → 33.92 Gcycles, 154.5 → 97.9 Ginstr.
5. **`feat(chunk): RawExternalChunk`**: adds `RawItem` (`encode`/`decode`) and `varint len | bytes` frames.
   An item that is wholly in the read buffer is decoded in place. Compared with the hand-written chunk on
   `io::Take`: 12.70 → 11.68 Gcycles.
6. **`feat(sort): optional bounded fan-in`**: adds `with_max_fan_in(n)`, which merges chunks generation by
   generation while the input is still being read. This is a robustness fix: with `ulimit -n 128`, 250 chunks
   used to fail with "Too many open files". It costs cycles: with an 8 MiB budget, 9.84 → 11.22 Gcycles at
   fan-in 16, while RSS drops from 204 MB to 178 MB. The default (no limit) is unchanged.
7. **`feat(record): RecordSorter`**: an arena-based sorter for `(K: RadixKey, &[u8])` records:
   - records are copied into one buffer per producer;
   - only a 24-byte packed `(key, offset, len)` index is sorted, using a stable LSD radix sort that skips
     constant digits;
   - the memory budget is counted in bytes;
   - runs are written as `key | varint | record`;
   - the merge uses a loser tree and lends each record;
   - fan-in is bounded, and intermediate passes run in parallel;
   - any number of `RecordBuffer`s can be filled from different threads;
   - a buffer dropped without `finish` still writes its run, and any error from that is reported by `merge`.

   The loser tree is shared with `LoserTreeMerger`. `RecordSorter`: 4.52 Gcycles, against 11.40 for
   `ExternalSorter` with `RawExternalChunk` (both totals, including key generation).
8. **`perf(record): lend records from the read buffer`**: run headers are parsed in place, and records are
   lent from the `BufReader` buffer instead of being copied. 4.52 → 4.30 Gcycles, 9.67 → 9.13 Ginstr.

## API changes

All additions are opt-in:
- `LoserTreeMerger`
- `ChunkReader`
- `RawExternalChunk` and `RawItem`
- `ExternalSorterBuilder::with_max_fan_in`
- `ChunkBuffer::is_empty`, which has a default
- `radix::RadixKey`
- `record::{RecordSorter, RecordSorterBuilder, RecordBuffer, RecordMerger}`

Changes that could affect existing code:
- **Return type of `sort`/`sort_by`**: these now return `LoserTreeMerger`. The deprecated alias
  `BinaryHeapMerger` keeps code that names the type compiling. The deprecation says `since = "0.2.0"`; the
  version number is left to the maintainers.
- **Merger error semantics**: after a chunk returns an error, that chunk is dropped from the merge and no items
  are lost. Previously, the popped item disappeared.
- **`RmpExternalChunk`**: its private field changed type. Its behavior is the same.

## Tried and dropped (measured, no gain)

- **Single-threaded `slice::sort_by`** instead of rayon's `par_sort_by` with one thread: 0.9 Ginstr fewer, but
  cycles unchanged.
- **`sort_by_key`** (radix sort of `(key, position)`, then gather): 19.9 vs 23.2 Ginstr, but 13.72 vs 13.19
  Gcycles. The random gather reads cost more than the sort they replace, because the items are only 48 bytes.
- **Byte-limited buffer with a `MemSize` trait** instead of `deepsize`: `deepsize` turned out not to be slow
  here (12.09 vs 12.14 Gcycles). Both were slower than the preallocated count limit (11.68), because of `Vec`
  growth and one extra chunk.
- **Encoding MessagePack into a `Vec` first**: much worse (44.7 vs 33.9 Gcycles). Related quirk: passing
  `&mut &mut BufWriter` to `rmp_serde::encode::write` takes about a third fewer instructions than passing
  `&mut BufWriter`. The code keeps the double reference and has a comment explaining why.
- **Skipping radix passes over low digits that arrive already ordered** (a sequence number in the key):
  3 passes fewer, but detecting it cost more than those passes saved (4.44 → 4.59 Gcycles).

## Remaining gap and why

`ExternalSorter` is still about 3x slower than `RecordSorter` on this data (10.8 vs 3.6 Gcycles). The cause is
the owned-item design, which a compatible change cannot remove:
- **Allocation (about 40% of cycles)**: each `Item(u128, Vec<u8>)` is allocated by the caller, freed when it is
  written, allocated again when it is read back, and freed by the consumer. The frees happen in sorted order,
  which is random in memory. That makes glibc's `malloc_consolidate` and `unlink_chunk` expensive.
- **Sorting (about 25–30% of cycles)**: the chunk sort is a comparison merge sort that moves 48-byte items.
  Sorting a key index instead needs a gather, which costs more (see above).

Reusing allocations would need a lending merger and a `decode_into(&mut T)` chunk method, which amounts to
`RecordSorter`'s design. With the default `RmpExternalChunk`, most of the remaining time is serde encoding
`Vec<u8>` as an array of 64 integers; `serde_bytes` on such fields is the user-side fix.

Known pre-existing issue: `cargo test` without `--all-features` fails the crate-level doctest, because that
doctest needs the `memory-limit` feature. CI runs with `--all-features`.
