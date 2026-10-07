[![Crates.io][crates-badge]][crates-url]
[![License][licence-badge]][licence-url]
[![Test Status][test-badge]][test-url]
[![Documentation][doc-badge]][doc-url]

[crates-badge]: https://img.shields.io/crates/v/ext-sort.svg
[crates-url]: https://crates.io/crates/ext-sort
[licence-badge]: https://img.shields.io/badge/license-Unlicense-blue.svg
[licence-url]: https://github.com/dapper91/ext-sort-rs/blob/master/LICENSE
[test-badge]: https://github.com/dapper91/ext-sort-rs/actions/workflows/test.yml/badge.svg?branch=master
[test-url]: https://github.com/dapper91/ext-sort-rs/actions/workflows/test.yml
[doc-badge]: https://docs.rs/ext-sort/badge.svg
[doc-url]: https://docs.rs/ext-sort


# Rust external sort

`ext-sort` is a rust external sort algorithm implementation.

External sorting is a class of sorting algorithms that can handle massive amounts of data. External sorting 
is required when the data being sorted do not fit into the main memory (RAM) of a computer and instead must be 
resided in slower external memory, usually a hard disk drive. Sorting is achieved in two passes. During the 
first pass it sorts chunks of data that each fit in RAM, during the second pass it merges the sorted chunks together. 
For more information see [External Sorting](https://en.wikipedia.org/wiki/External_sorting).

## Overview

`ext-sort` supports the following features:

* **Data agnostic:**
  it supports all data types that implement `serde` serialization/deserialization by default,
  otherwise you can implement your own serialization/deserialization mechanism.
* **Serialization format agnostic:**
  the library uses `MessagePack` serialization format by default, but it can be easily substituted by your custom one
  if `MessagePack` serialization/deserialization performance is not sufficient for your task.
* **Multithreading support:**
  multi-threaded sorting is supported, which means data is sorted in multiple threads utilizing maximum CPU resources
  and reducing sorting time.
* **Memory limit support:**
  memory limited sorting is supported. It allows you to limit sorting memory consumption
  (`memory-limit` feature required). 
* **Bounded fan-in:**
  `ExternalSorterBuilder::with_max_fan_in` limits how many chunks are open and merged at once, merging
  chunks in intermediate passes when there are more.
* **Fast binary chunks:**
  `RawExternalChunk` stores items implementing the small `RawItem` trait as `length | bytes`, without a
  serialization framework, and `ChunkReader` speeds up custom chunk implementations.
* **Record sorter:**
  `record::RecordSorter` sorts `(key, bytes)` records without allocating per record: records are kept in one
  buffer per producer, only a compact index is radix sorted, several threads can push concurrently, and the
  merger lends each record. For data of that shape it is several times faster than `ExternalSorter`.

# Basic example

Activate `memory-limit` feature of the ext-sort crate on Cargo.toml:

```toml
[dependencies]
ext-sort = { version = "^0.1.6", features = ["memory-limit"] }
```

```rust
use std::fs;
use std::io::{self, prelude::*};
use std::path;

use bytesize::MB;
use env_logger;
use log;

use ext_sort::{buffer::mem::MemoryLimitedBufferBuilder, ExternalSorter, ExternalSorterBuilder};

fn main() {
    env_logger::Builder::new().filter_level(log::LevelFilter::Debug).init();

    let input_reader = io::BufReader::new(fs::File::open("input.txt").unwrap());
    let mut output_writer = io::BufWriter::new(fs::File::create("output.txt").unwrap());

    let sorter: ExternalSorter<String, io::Error, MemoryLimitedBufferBuilder> = ExternalSorterBuilder::new()
        .with_tmp_dir(path::Path::new("./"))
        .with_buffer(MemoryLimitedBufferBuilder::new(50 * MB))
        .build()
        .unwrap();

    let sorted = sorter.sort(input_reader.lines()).unwrap();

    for item in sorted.map(Result::unwrap) {
        output_writer.write_all(format!("{}\n", item).as_bytes()).unwrap();
    }
    output_writer.flush().unwrap();
}
```

# Record sorter example

```rust
use ext_sort::record::RecordSorterBuilder;

let sorter = RecordSorterBuilder::new().with_buffer_bytes(64 << 20).build::<u64>().unwrap();
let mut buffer = sorter.buffer();
for (key, name) in [(3, "three"), (1, "one"), (2, "two")] {
    buffer.push(key, name.as_bytes()).unwrap();
}
buffer.finish().unwrap();

let mut merger = sorter.merge().unwrap();
while let Some((key, record)) = merger.next().unwrap() {
    println!("{key}: {}", String::from_utf8_lossy(record));
}
```

# Benchmark

`examples/bench.rs` sorts 10M records (a `u128` key and a 64-byte payload) with a 256 MiB budget on one thread,
one mode per process, e.g.:

```sh
cargo build --release --example bench
perf stat -e cycles:u,instructions:u target/release/examples/bench record 10000000
```
