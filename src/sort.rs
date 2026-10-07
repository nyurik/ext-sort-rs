//! External sorter.

use log;
use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::fmt::{Debug, Display};
use std::io;
use std::marker::PhantomData;
use std::path::Path;

use crate::chunk::{ExternalChunk, ExternalChunkError, RmpExternalChunk};
use crate::merger::LoserTreeMerger;
use crate::{ChunkBuffer, ChunkBufferBuilder, LimitedBufferBuilder};

/// Sorting error.
#[derive(Debug)]
pub enum SortError<S: Error, D: Error, I: Error> {
    /// Temporary directory or file creation error.
    TempDir(io::Error),
    /// Workers thread pool initialization error.
    ThreadPoolBuildError(rayon::ThreadPoolBuildError),
    /// Common I/O error.
    IO(io::Error),
    /// Data serialization error.
    SerializationError(S),
    /// Data deserialization error.
    DeserializationError(D),
    /// Input data stream error
    InputError(I),
}

impl<S, D, I> Error for SortError<S, D, I>
where
    S: Error + 'static,
    D: Error + 'static,
    I: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match &self {
            SortError::TempDir(err) => err,
            SortError::ThreadPoolBuildError(err) => err,
            SortError::IO(err) => err,
            SortError::SerializationError(err) => err,
            SortError::DeserializationError(err) => err,
            SortError::InputError(err) => err,
        })
    }
}

impl<S: Error, D: Error, I: Error> Display for SortError<S, D, I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            SortError::TempDir(err) => write!(f, "temporary directory or file not created: {}", err),
            SortError::ThreadPoolBuildError(err) => write!(f, "thread pool initialization failed: {}", err),
            SortError::IO(err) => write!(f, "I/O operation failed: {}", err),
            SortError::SerializationError(err) => write!(f, "data serialization error: {}", err),
            SortError::DeserializationError(err) => write!(f, "data deserialization error: {}", err),
            SortError::InputError(err) => write!(f, "input data stream error: {}", err),
        }
    }
}

/// [`SortError`] parameterized by the error types of external chunk `C`.
type ChunkSortError<T, E, C> =
    SortError<<C as ExternalChunk<T>>::SerializationError, <C as ExternalChunk<T>>::DeserializationError, E>;

/// Result of a sorter operation producing `R`.
type SortResult<R, T, E, C> = Result<R, ChunkSortError<T, E, C>>;

/// External sorter builder. Provides methods for [`ExternalSorter`] initialization.
#[derive(Clone)]
pub struct ExternalSorterBuilder<T, E, B = LimitedBufferBuilder, C = RmpExternalChunk<T>>
where
    T: Send,
    E: Error,
    B: ChunkBufferBuilder<T>,
    C: ExternalChunk<T>,
{
    /// Number of threads to be used to sort data in parallel.
    threads_number: Option<usize>,
    /// Directory to be used to store temporary data.
    tmp_dir: Option<Box<Path>>,
    /// Chunk file read/write buffer size.
    rw_buf_size: Option<usize>,
    /// Chunk buffer builder.
    buffer_builder: B,
    /// Maximum number of chunks merged at once.
    max_fan_in: Option<usize>,

    /// External chunk type.
    external_chunk_type: PhantomData<C>,
    /// Input item type.
    item_type: PhantomData<T>,
    /// Input error type.
    input_error_type: PhantomData<E>,
}

impl<T, E, B, C> ExternalSorterBuilder<T, E, B, C>
where
    T: Send,
    E: Error,
    B: ChunkBufferBuilder<T>,
    C: ExternalChunk<T>,
{
    /// Creates an instance of a builder with default parameters.
    pub fn new() -> Self {
        ExternalSorterBuilder::default()
    }

    /// Builds an [`ExternalSorter`] instance using provided configuration.
    pub fn build(self) -> SortResult<ExternalSorter<T, E, B, C>, T, E, C> {
        let mut sorter = ExternalSorter::new(
            self.threads_number,
            self.tmp_dir.as_deref(),
            self.buffer_builder,
            self.rw_buf_size,
        )?;
        sorter.max_fan_in = self.max_fan_in;
        Ok(sorter)
    }

    /// Sets number of threads to be used to sort data in parallel.
    pub fn with_threads_number(mut self, threads_number: usize) -> ExternalSorterBuilder<T, E, B, C> {
        self.threads_number = Some(threads_number);
        self
    }

    /// Sets directory to be used to store temporary data.
    pub fn with_tmp_dir(mut self, path: &Path) -> ExternalSorterBuilder<T, E, B, C> {
        self.tmp_dir = Some(path.into());
        self
    }

    /// Sets buffer builder.
    pub fn with_buffer(mut self, buffer_builder: B) -> ExternalSorterBuilder<T, E, B, C> {
        self.buffer_builder = buffer_builder;
        self
    }

    /// Sets the maximum number of chunks merged at once, at least 2. By default all chunks are merged at
    /// once, which keeps every chunk file and its read buffer open until the sorted output is consumed.
    ///
    /// With a limit, whenever `max_fan_in` chunks of the same generation exist they are merged into a single
    /// chunk of the next generation while the input is still being read, and any excess left at the end is
    /// merged before the final merge. So at most about `max_fan_in` times the number of generations chunks
    /// are open at any time, and each item is rewritten once per generation.
    pub fn with_max_fan_in(mut self, max_fan_in: usize) -> ExternalSorterBuilder<T, E, B, C> {
        self.max_fan_in = Some(max_fan_in.max(2));
        self
    }

    /// Sets chunk read/write buffer size.
    pub fn with_rw_buf_size(mut self, buf_size: usize) -> ExternalSorterBuilder<T, E, B, C> {
        self.rw_buf_size = Some(buf_size);
        self
    }
}

impl<T, E, B, C> Default for ExternalSorterBuilder<T, E, B, C>
where
    T: Send,
    E: Error,
    B: ChunkBufferBuilder<T>,
    C: ExternalChunk<T>,
{
    fn default() -> Self {
        ExternalSorterBuilder {
            threads_number: None,
            tmp_dir: None,
            rw_buf_size: None,
            buffer_builder: B::default(),
            max_fan_in: None,
            external_chunk_type: PhantomData,
            item_type: PhantomData,
            input_error_type: PhantomData,
        }
    }
}

/// External sorter.
pub struct ExternalSorter<T, E, B = LimitedBufferBuilder, C = RmpExternalChunk<T>>
where
    T: Send,
    E: Error,
    B: ChunkBufferBuilder<T>,
    C: ExternalChunk<T>,
{
    /// Sorting thread pool.
    thread_pool: rayon::ThreadPool,
    /// Directory to be used to store temporary data.
    tmp_dir: tempfile::TempDir,
    /// Chunk buffer builder.
    buffer_builder: B,
    /// Chunk file read/write buffer size.
    rw_buf_size: Option<usize>,
    /// Maximum number of chunks merged at once.
    max_fan_in: Option<usize>,

    /// External chunk type.
    external_chunk_type: PhantomData<C>,
    /// Input item type.
    item_type: PhantomData<T>,
    /// Input error type.
    input_error_type: PhantomData<E>,
}

impl<T, E, B, C> ExternalSorter<T, E, B, C>
where
    T: Send,
    E: Error,
    B: ChunkBufferBuilder<T>,
    C: ExternalChunk<T>,
{
    /// Creates a new external sorter instance.
    ///
    /// # Arguments
    /// * `threads_number` - Number of threads to be used to sort data in parallel. If the parameter is [`None`]
    ///   threads number will be selected based on available CPU core number.
    /// * `tmp_path` - Directory to be used to store temporary data. If paramater is [`None`] default OS temporary
    ///   directory will be used.
    /// * `buffer_builder` - An instance of a buffer builder that will be used for chunk buffer creation.
    /// * `rw_buf_size` - Chunks file read/write buffer size.
    pub fn new(
        threads_number: Option<usize>,
        tmp_path: Option<&Path>,
        buffer_builder: B,
        rw_buf_size: Option<usize>,
    ) -> Result<Self, ChunkSortError<T, E, C>> {
        Ok(ExternalSorter {
            rw_buf_size,
            buffer_builder,
            max_fan_in: None,
            thread_pool: Self::init_thread_pool(threads_number)?,
            tmp_dir: Self::init_tmp_directory(tmp_path)?,
            external_chunk_type: PhantomData,
            item_type: PhantomData,
            input_error_type: PhantomData,
        })
    }

    fn init_thread_pool(threads_number: Option<usize>) -> Result<rayon::ThreadPool, ChunkSortError<T, E, C>> {
        let mut thread_pool_builder = rayon::ThreadPoolBuilder::new();

        if let Some(threads_number) = threads_number {
            log::info!("initializing thread-pool (threads: {})", threads_number);
            thread_pool_builder = thread_pool_builder.num_threads(threads_number);
        } else {
            log::info!("initializing thread-pool (threads: default)");
        }
        let thread_pool = thread_pool_builder
            .build()
            .map_err(|err| SortError::ThreadPoolBuildError(err))?;

        Ok(thread_pool)
    }

    fn init_tmp_directory(tmp_path: Option<&Path>) -> Result<tempfile::TempDir, ChunkSortError<T, E, C>> {
        let tmp_dir = if let Some(tmp_path) = tmp_path {
            tempfile::tempdir_in(tmp_path)
        } else {
            tempfile::tempdir()
        }
        .map_err(|err| SortError::TempDir(err))?;

        log::info!("using {} as a temporary directory", tmp_dir.path().display());

        Ok(tmp_dir)
    }

    /// Sorts data from the input.
    /// Returns an iterator that can be used to get sorted data stream.
    ///
    /// # Arguments
    /// * `input` - Input stream data to be fetched from
    #[allow(clippy::type_complexity)] // the `impl Fn` cannot be named by an alias
    pub fn sort<I>(
        &self,
        input: I,
    ) -> SortResult<LoserTreeMerger<T, C::DeserializationError, impl Fn(&T, &T) -> Ordering + Copy, C>, T, E, C>
    where
        T: Ord,
        I: IntoIterator<Item = Result<T, E>>,
    {
        self.sort_by(input, T::cmp)
    }

    /// Sorts data from the input using a custom compare function.
    /// Returns an iterator that can be used to get sorted data stream.
    ///
    /// # Arguments
    /// * `input` - Input stream data to be fetched from
    /// * `compare` - Function be be used to compare items
    pub fn sort_by<I, F>(
        &self,
        input: I,
        compare: F,
    ) -> SortResult<LoserTreeMerger<T, C::DeserializationError, F, C>, T, E, C>
    where
        I: IntoIterator<Item = Result<T, E>>,
        F: Fn(&T, &T) -> Ordering + Sync + Send + Copy,
    {
        let mut runs = Vec::new();
        let mut chunk_buf = self.buffer_builder.build();

        for item in input.into_iter() {
            match item {
                Ok(item) => chunk_buf.push(item),
                Err(err) => return Err(SortError::InputError(err)),
            }

            if chunk_buf.is_full() {
                runs.push(Run {
                    chunk: self.create_chunk(chunk_buf, compare)?,
                    generation: 0,
                });
                chunk_buf = self.buffer_builder.build();
                self.merge_full_generations(&mut runs, compare)?;
            }
        }

        if !chunk_buf.is_empty() {
            runs.push(Run {
                chunk: self.create_chunk(chunk_buf, compare)?,
                generation: 0,
            });
        }

        if let Some(fan_in) = self.max_fan_in {
            while runs.len() > fan_in {
                // Merging just enough of the latest, smallest runs leaves exactly `fan_in` for the final merge.
                let count = (runs.len() - fan_in + 1).min(fan_in);
                self.merge_last_runs(&mut runs, count, compare)?;
            }
        }

        log::debug!("external sort preparation done");

        Ok(LoserTreeMerger::new(runs.into_iter().map(|run| run.chunk), compare))
    }

    fn create_chunk<F>(&self, mut buffer: impl ChunkBuffer<T>, compare: F) -> Result<C, ChunkSortError<T, E, C>>
    where
        F: Fn(&T, &T) -> Ordering + Sync + Send,
    {
        log::debug!("sorting chunk data ...");
        self.thread_pool.install(|| {
            buffer.par_sort_by(compare);
        });

        log::debug!("saving chunk data");
        self.write_chunk(buffer)
    }

    fn write_chunk(&self, items: impl IntoIterator<Item = T>) -> Result<C, ChunkSortError<T, E, C>> {
        ExternalChunk::build(&self.tmp_dir, items, self.rw_buf_size).map_err(|err| match err {
            ExternalChunkError::IO(err) => SortError::IO(err),
            ExternalChunkError::SerializationError(err) => SortError::SerializationError(err),
        })
    }

    /// Merges the latest runs while `max_fan_in` of them belong to the same generation, like carrying in a
    /// counter, so the number of open runs grows only logarithmically with the input.
    fn merge_full_generations<F>(&self, runs: &mut Vec<Run<C>>, compare: F) -> Result<(), ChunkSortError<T, E, C>>
    where
        F: Fn(&T, &T) -> Ordering,
    {
        let Some(fan_in) = self.max_fan_in else {
            return Ok(());
        };
        while let Some(last) = runs.last() {
            let generation = last.generation;
            let same = runs.iter().rev().take_while(|run| run.generation == generation).count();
            if same < fan_in {
                break;
            }
            self.merge_last_runs(runs, fan_in, &compare)?;
        }
        Ok(())
    }

    /// Replaces the last `count` runs with a single run holding their merged items. The runs are adjacent,
    /// and the merger breaks ties by run order, so the sort stays stable.
    fn merge_last_runs<F>(
        &self,
        runs: &mut Vec<Run<C>>,
        count: usize,
        compare: F,
    ) -> Result<(), ChunkSortError<T, E, C>>
    where
        F: Fn(&T, &T) -> Ordering,
    {
        let merged = runs.split_off(runs.len() - count);
        let generation = merged.iter().map(|run| run.generation).max().unwrap_or_default() + 1;
        log::debug!("merging {} chunks into a generation {} chunk", count, generation);

        let mut error = None;
        let items = LoserTreeMerger::new(merged.into_iter().map(|run| run.chunk), compare)
            .map_while(|item| item.map_err(|err| error = Some(err)).ok());
        let chunk = self.write_chunk(items)?;
        if let Some(err) = error {
            return Err(SortError::DeserializationError(err));
        }
        runs.push(Run { chunk, generation });
        Ok(())
    }
}

/// A sorted chunk and the number of merges its items went through.
struct Run<C> {
    chunk: C,
    generation: u32,
}

#[cfg(test)]
mod test {
    use std::io;
    use std::path::Path;

    use rand::seq::SliceRandom;
    use rstest::*;

    use super::{ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder};

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_external_sorter(#[case] reversed: bool) {
        let input_sorted = 0..100;

        let mut input_shuffled = Vec::from_iter(input_sorted.clone());
        input_shuffled.shuffle(&mut rand::thread_rng());

        let input: Vec<Result<i32, io::Error>> = Vec::from_iter(input_shuffled.into_iter().map(Ok));

        let sorter: ExternalSorter<i32, _> = ExternalSorterBuilder::new()
            .with_buffer(LimitedBufferBuilder::new(8, true))
            .with_threads_number(2)
            .with_tmp_dir(Path::new("./"))
            .build()
            .unwrap();

        let compare = if reversed {
            |a: &i32, b: &i32| a.cmp(b).reverse()
        } else {
            |a: &i32, b: &i32| a.cmp(b)
        };

        let result = sorter.sort_by(input, compare).unwrap();

        let actual_result: Result<Vec<i32>, _> = result.collect();
        let actual_result = actual_result.unwrap();
        let expected_result = if reversed {
            Vec::from_iter(input_sorted.clone().rev())
        } else {
            Vec::from_iter(input_sorted.clone())
        };

        assert_eq!(actual_result, expected_result)
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_external_sorter_stability(#[case] reversed: bool) {
        let input_sorted = (0..20).flat_map(|x| (0..5).map(move |y| (x, y)));

        let mut input_shuffled = Vec::from_iter(input_sorted.clone());
        input_shuffled.shuffle(&mut rand::thread_rng());
        // sort input by the second field to check sorting stability
        input_shuffled.sort_by(|a: &(i32, i32), b: &(i32, i32)| {
            if reversed {
                a.1.cmp(&b.1).reverse()
            } else {
                a.1.cmp(&b.1)
            }
        });

        let input: Vec<Result<(i32, i32), io::Error>> = Vec::from_iter(input_shuffled.into_iter().map(Ok));

        let sorter: ExternalSorter<(i32, i32), _> = ExternalSorterBuilder::new()
            .with_buffer(LimitedBufferBuilder::new(8, true))
            .with_threads_number(2)
            .with_tmp_dir(Path::new("./"))
            .build()
            .unwrap();

        let compare = if reversed {
            |a: &(i32, i32), b: &(i32, i32)| a.0.cmp(&b.0).reverse()
        } else {
            |a: &(i32, i32), b: &(i32, i32)| a.0.cmp(&b.0)
        };

        let result = sorter.sort_by(input, compare).unwrap();

        let actual_result: Result<Vec<(i32, i32)>, _> = result.collect();
        let actual_result = actual_result.unwrap();
        let expected_result = if reversed {
            Vec::from_iter(input_sorted.clone().rev())
        } else {
            Vec::from_iter(input_sorted.clone())
        };

        assert_eq!(actual_result, expected_result)
    }

    #[rstest]
    #[case(None, 1000)]
    #[case(Some(2), 1000)]
    #[case(Some(3), 1000)]
    #[case(Some(5), 1000)]
    #[case(Some(3), 0)]
    #[case(Some(3), 5)]
    fn test_external_sorter_fan_in(#[case] max_fan_in: Option<usize>, #[case] len: usize) {
        // few distinct keys, and the input position as payload, so stability is checked across merges
        let mut rng = rand::thread_rng();
        let input: Vec<(u8, usize)> = (0..len)
            .map(|pos| (*[3, 1, 4, 1, 5].choose(&mut rng).unwrap(), pos))
            .collect();
        let mut expected = input.clone();
        expected.sort_by_key(|item| item.0);

        let mut builder = ExternalSorterBuilder::new()
            .with_buffer(LimitedBufferBuilder::new(7, true))
            .with_threads_number(1)
            .with_tmp_dir(Path::new("./"));
        if let Some(max_fan_in) = max_fan_in {
            builder = builder.with_max_fan_in(max_fan_in);
        }
        let sorter: ExternalSorter<(u8, usize), io::Error> = builder.build().unwrap();

        let result = sorter.sort_by(input.into_iter().map(Ok), |a, b| a.0.cmp(&b.0)).unwrap();
        let chunks = len.div_ceil(7);
        assert_eq!(result.inputs(), max_fan_in.map_or(chunks, |fan_in| chunks.min(fan_in)));
        let actual: Vec<_> = result.map(Result::unwrap).collect();
        assert_eq!(actual, expected);
    }
}
