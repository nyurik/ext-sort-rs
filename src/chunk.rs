//! External chunk.

use std::error::Error;
use std::fmt::{self, Display};
use std::fs;
use std::io;
use std::io::prelude::*;
use std::marker::PhantomData;

use tempfile;

/// External chunk error
#[derive(Debug)]
pub enum ExternalChunkError<S: Error> {
    /// Common I/O error.
    IO(io::Error),
    /// Data serialization error.
    SerializationError(S),
}

impl<S: Error> Error for ExternalChunkError<S> {}

impl<S: Error> Display for ExternalChunkError<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExternalChunkError::IO(err) => write!(f, "{}", err),
            ExternalChunkError::SerializationError(err) => write!(f, "{}", err),
        }
    }
}

impl<S: Error> From<io::Error> for ExternalChunkError<S> {
    fn from(err: io::Error) -> Self {
        ExternalChunkError::IO(err)
    }
}

/// External chunk interface. Provides methods for creating a chunk stored on file system and reading data from it.
pub trait ExternalChunk<T>: Sized + Iterator<Item = Result<T, Self::DeserializationError>> {
    /// Error returned when data serialization failed.
    type SerializationError: Error;
    /// Error returned when data deserialization failed.
    type DeserializationError: Error;

    /// Builds an instance of an external chunk creating file and dumping the items to it.
    ///
    /// # Arguments
    /// * `dir` - Directory the chunk file is created in
    /// * `items` - Items to be dumped to the chunk
    /// * `buf_size` - File I/O buffer size
    fn build(
        dir: &tempfile::TempDir,
        items: impl IntoIterator<Item = T>,
        buf_size: Option<usize>,
    ) -> Result<Self, ExternalChunkError<Self::SerializationError>> {
        let tmp_file = tempfile::tempfile_in(dir)?;

        let mut chunk_writer = match buf_size {
            Some(buf_size) => io::BufWriter::with_capacity(buf_size, tmp_file.try_clone()?),
            None => io::BufWriter::new(tmp_file.try_clone()?),
        };

        Self::dump(&mut chunk_writer, items).map_err(ExternalChunkError::SerializationError)?;

        chunk_writer.flush()?;

        let mut chunk_reader = match buf_size {
            Some(buf_size) => io::BufReader::with_capacity(buf_size, tmp_file.try_clone()?),
            None => io::BufReader::new(tmp_file.try_clone()?),
        };

        chunk_reader.rewind()?;
        let file_len = tmp_file.metadata()?.len();

        Ok(Self::new(chunk_reader.take(file_len)))
    }

    /// Creates and instance of an external chunk.
    ///
    /// # Arguments
    /// * `reader` - The reader of the file the chunk is stored in
    fn new(reader: io::Take<io::BufReader<fs::File>>) -> Self;

    /// Dumps items to an external file.
    ///
    /// # Arguments
    /// * `chunk_writer` - The writer of the file the data should be dumped in
    /// * `items` - Items to be dumped
    fn dump(
        chunk_writer: &mut io::BufWriter<fs::File>,
        items: impl IntoIterator<Item = T>,
    ) -> Result<(), Self::SerializationError>;
}

/// Reader of a chunk file: a buffered reader limited to the chunk length, like [`io::Take`].
///
/// Unlike `io::Take`, it forwards [`Read::read_exact`] to the buffered reader, whose fast path copies
/// straight from its buffer. `io::Take` runs the generic loop of `read` calls instead, which dominates
/// the cost of decoders that read a few bytes at a time, such as MessagePack. Custom [`ExternalChunk`]
/// implementations can convert the reader they are given with [`From`].
pub struct ChunkReader {
    inner: io::BufReader<fs::File>,
    remaining: u64,
}

impl ChunkReader {
    /// Returns the number of chunk bytes left to read.
    #[inline]
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Checks if the whole chunk has been read.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.remaining == 0
    }
}

impl From<io::Take<io::BufReader<fs::File>>> for ChunkReader {
    fn from(reader: io::Take<io::BufReader<fs::File>>) -> Self {
        let remaining = reader.limit();
        ChunkReader {
            inner: reader.into_inner(),
            remaining,
        }
    }
}

impl Read for ChunkReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = buf.len().min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        let read = self.inner.read(&mut buf[..len])?;
        self.remaining -= read as u64;
        Ok(read)
    }

    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        if buf.len() as u64 > self.remaining {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "chunk ended in the middle of an item",
            ));
        }
        self.inner.read_exact(buf)?;
        self.remaining -= buf.len() as u64;
        Ok(())
    }
}

impl BufRead for ChunkReader {
    #[inline]
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.remaining == 0 {
            return Ok(&[]);
        }
        let buf = self.inner.fill_buf()?;
        Ok(&buf[..buf.len().min(usize::try_from(self.remaining).unwrap_or(usize::MAX))])
    }

    #[inline]
    fn consume(&mut self, amt: usize) {
        let amt = amt.min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        self.inner.consume(amt);
        self.remaining -= amt as u64;
    }
}

/// RMP (Rust MessagePack) external chunk implementation.
/// It uses MessagePack as a data serialization format.
/// For more information see [msgpack.org](https://msgpack.org/).
///
/// # Example
///
/// ```no_run
/// use tempfile::TempDir;
/// use ext_sort::{ExternalChunk, RmpExternalChunk};
///
/// let dir = TempDir::new().unwrap();
/// let chunk: RmpExternalChunk<i32> = ExternalChunk::build(&dir, (0..1000), None).unwrap();
/// ```
pub struct RmpExternalChunk<T> {
    reader: ChunkReader,

    item_type: PhantomData<T>,
}

impl<T> ExternalChunk<T> for RmpExternalChunk<T>
where
    T: serde::ser::Serialize + serde::de::DeserializeOwned,
{
    type SerializationError = rmp_serde::encode::Error;
    type DeserializationError = rmp_serde::decode::Error;

    fn new(reader: io::Take<io::BufReader<fs::File>>) -> Self {
        RmpExternalChunk {
            reader: reader.into(),
            item_type: PhantomData,
        }
    }

    fn dump(
        mut chunk_writer: &mut io::BufWriter<fs::File>,
        items: impl IntoIterator<Item = T>,
    ) -> Result<(), Self::SerializationError> {
        for item in items {
            // Passing `&mut &mut BufWriter` is deliberate: it inlines better than `&mut BufWriter`, which
            // measured about a third more instructions.
            rmp_serde::encode::write(&mut chunk_writer, &item)?;
        }

        Ok(())
    }
}

impl<T> Iterator for RmpExternalChunk<T>
where
    T: serde::ser::Serialize + serde::de::DeserializeOwned,
{
    type Item = Result<T, <Self as ExternalChunk<T>>::DeserializationError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.is_empty() {
            None
        } else {
            match rmp_serde::decode::from_read(&mut self.reader) {
                Ok(result) => Some(Ok(result)),
                Err(err) => Some(Err(err)),
            }
        }
    }
}

/// Item with a compact binary encoding, stored by [`RawExternalChunk`].
pub trait RawItem: Sized {
    /// Appends the encoded item to `buf`.
    fn encode(&self, buf: &mut Vec<u8>);

    /// Decodes an item from exactly the bytes [`RawItem::encode`] appended.
    fn decode(bytes: &[u8]) -> io::Result<Self>;
}

impl RawItem for Vec<u8> {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        Ok(bytes.to_vec())
    }
}

impl RawItem for String {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        String::from_utf8(bytes.to_vec()).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    }
}

/// External chunk storing [`RawItem`]s as `varint length | encoded item`, without a serialization
/// framework. Items that are wholly in the read buffer are decoded in place, without copying them first.
///
/// # Example
///
/// ```no_run
/// use tempfile::TempDir;
/// use ext_sort::{ExternalChunk, RawExternalChunk};
///
/// let dir = TempDir::new().unwrap();
/// let items = vec![b"hello".to_vec(), b"world".to_vec()];
/// let chunk: RawExternalChunk<Vec<u8>> = ExternalChunk::build(&dir, items, None).unwrap();
/// ```
pub struct RawExternalChunk<T> {
    reader: ChunkReader,
    /// Holds items that straddle the end of the read buffer.
    scratch: Vec<u8>,

    item_type: PhantomData<T>,
}

impl<T: RawItem> RawExternalChunk<T> {
    fn read_item(&mut self) -> io::Result<T> {
        let len = read_varint(&mut self.reader)?;
        let len = usize::try_from(len).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let buf = self.reader.fill_buf()?;
        if let Some(bytes) = buf.get(..len) {
            let item = T::decode(bytes)?;
            self.reader.consume(len);
            return Ok(item);
        }
        self.scratch.resize(len, 0);
        self.reader.read_exact(&mut self.scratch)?;
        T::decode(&self.scratch)
    }
}

impl<T: RawItem> ExternalChunk<T> for RawExternalChunk<T> {
    type SerializationError = io::Error;
    type DeserializationError = io::Error;

    fn new(reader: io::Take<io::BufReader<fs::File>>) -> Self {
        RawExternalChunk {
            reader: reader.into(),
            scratch: Vec::new(),
            item_type: PhantomData,
        }
    }

    fn dump(chunk_writer: &mut io::BufWriter<fs::File>, items: impl IntoIterator<Item = T>) -> io::Result<()> {
        // The varint length is written first, so each item is encoded into a reused buffer before writing.
        let mut encoded = Vec::new();
        let mut frame = Vec::with_capacity(MAX_VARINT_LEN);
        for item in items {
            encoded.clear();
            item.encode(&mut encoded);
            frame.clear();
            write_varint(&mut frame, encoded.len() as u64);
            chunk_writer.write_all(&frame)?;
            chunk_writer.write_all(&encoded)?;
        }
        Ok(())
    }
}

impl<T: RawItem> Iterator for RawExternalChunk<T> {
    type Item = io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.is_empty() {
            None
        } else {
            Some(self.read_item())
        }
    }
}

const MAX_VARINT_LEN: usize = 10;

/// Appends `value` as a LEB128 varint: 7 bits per byte, least significant first.
fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push(value as u8 | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

fn read_varint(reader: &mut impl Read) -> io::Result<u64> {
    let mut value = 0;
    for shift in (0..64).step_by(7) {
        let mut byte = [0];
        reader.read_exact(&mut byte)?;
        value |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] < 0x80 {
            return Ok(value);
        }
    }
    Err(io::Error::new(io::ErrorKind::InvalidData, "varint is too long"))
}

#[cfg(test)]
mod test {
    use rstest::*;

    use std::io::{self, prelude::*};

    use super::{
        read_varint, write_varint, ChunkReader, ExternalChunk, RawExternalChunk, RmpExternalChunk, MAX_VARINT_LEN,
    };

    #[fixture]
    fn tmp_dir() -> tempfile::TempDir {
        tempfile::tempdir_in("./").unwrap()
    }

    #[rstest]
    fn test_chunk_reader(tmp_dir: tempfile::TempDir) {
        let path = tmp_dir.path().join("data");
        std::fs::write(&path, b"0123456789").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        // a reader buffer smaller than the data exercises both the buffered and the refill paths
        let mut reader = ChunkReader::from(io::BufReader::with_capacity(4, file).take(8));

        let mut buf = [0; 3];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"012");
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"345");
        assert_eq!(reader.remaining(), 2);
        assert_eq!(
            reader.read_exact(&mut buf).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert_eq!(reader.fill_buf().unwrap(), b"67");
        reader.consume(1);
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).unwrap();
        assert_eq!(rest, b"7");
        assert!(reader.is_empty());
        assert_eq!(reader.fill_buf().unwrap(), b"");
    }

    #[rstest]
    fn test_raw_chunk(tmp_dir: tempfile::TempDir) {
        // Lengths up to 300 bytes need two varint bytes, and a 64 byte buffer makes items straddle refills.
        let saved: Vec<Vec<u8>> = (0..300).map(|len| vec![len as u8; len]).collect();
        let chunk: RawExternalChunk<Vec<u8>> = ExternalChunk::build(&tmp_dir, saved.clone(), Some(64)).unwrap();
        let restored: Result<Vec<_>, _> = chunk.collect();
        assert_eq!(restored.unwrap(), saved);

        let chunk: RawExternalChunk<String> = ExternalChunk::build(&tmp_dir, Vec::<String>::new(), None).unwrap();
        assert_eq!(chunk.count(), 0);
    }

    #[test]
    fn test_varint() {
        for value in [0, 1, 0x7f, 0x80, 300, u32::MAX.into(), u64::MAX] {
            let mut buf = Vec::new();
            write_varint(&mut buf, value);
            assert!(buf.len() <= MAX_VARINT_LEN);
            assert_eq!(read_varint(&mut buf.as_slice()).unwrap(), value);
        }
        assert!(read_varint(&mut [0xff; 11].as_slice()).is_err());
    }

    #[rstest]
    fn test_rmp_chunk(tmp_dir: tempfile::TempDir) {
        let saved = Vec::from_iter(0..100);

        let chunk: RmpExternalChunk<i32> = ExternalChunk::build(&tmp_dir, saved.clone(), None).unwrap();

        let restored: Result<Vec<i32>, _> = chunk.collect();
        let restored = restored.unwrap();

        assert_eq!(restored, saved);
    }
}
