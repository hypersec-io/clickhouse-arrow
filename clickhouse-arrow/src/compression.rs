//! Compression/decompression for ClickHouse native protocol.
//!
//! LZ4 and ZSTD support w/ ClickHouse's custom frame format:
//! - 16 bytes: CityHash128 checksum
//! - 1 byte: compression method (0x82=LZ4, 0x90=ZSTD)
//! - 4 bytes: compressed size (incl. 9-byte header)
//! - 4 bytes: decompressed size
//! - N bytes: payload
//!
//! Checksum covers method+sizes+payload. Matches clickhouse-rs and official C++ client.
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::FutureExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};

use crate::io::{ClickHouseRead, ClickHouseWrite};
use crate::native::protocol::CompressionMethod;
use crate::{Error, Result};

/// Compress and write in ClickHouse chunk format.
#[expect(clippy::cast_possible_truncation)]
#[cfg_attr(not(test), expect(unused))]
pub(crate) async fn compress_data<W: ClickHouseWrite>(
    writer: &mut W,
    raw: Vec<u8>,
    compression: CompressionMethod,
) -> Result<()> {
    let decompressed_size = raw.len();
    let mut out = match compression {
        // ZSTD with default compression level (1)
        CompressionMethod::ZSTD => zstd::bulk::compress(&raw, 1)
            .map_err(|e| Error::SerializeError(format!("ZSTD compress error: {e}")))?,
        // LZ4
        CompressionMethod::LZ4 => lz4_flex::compress(&raw),
        // None
        CompressionMethod::None => return Ok(()),
    };

    let mut new_out = Vec::with_capacity(out.len() + 13);
    new_out.push(compression.byte());
    new_out.extend_from_slice(&(out.len() as u32 + 9).to_le_bytes()[..]);
    new_out.extend_from_slice(&(decompressed_size as u32).to_le_bytes()[..]);
    new_out.append(&mut out);

    let hash = cityhash_rs::cityhash_102_128(&new_out[..]);
    writer.write_u64_le((hash >> 64) as u64).await?;
    writer.write_u64_le(hash as u64).await?;
    writer.write_all(&new_out[..]).await?;

    Ok(())
}

#[expect(clippy::cast_possible_truncation)]
pub(crate) async fn compress_data_sync<W: ClickHouseWrite>(
    writer: &mut W,
    raw: bytes::Bytes,
    compression: CompressionMethod,
) -> Result<()> {
    let decompressed_size = raw.len();
    let mut out = match compression {
        // ZSTD with default compression level (1)
        CompressionMethod::ZSTD => zstd::bulk::compress(&raw, 1)
            .map_err(|e| Error::SerializeError(format!("ZSTD compress error: {e}")))?,
        // LZ4
        CompressionMethod::LZ4 => lz4_flex::compress(&raw),
        // None
        CompressionMethod::None => return Ok(()),
    };

    let mut new_out = Vec::with_capacity(out.len() + 13);
    new_out.push(compression.byte());
    new_out.extend_from_slice(&(out.len() as u32 + 9).to_le_bytes()[..]);
    new_out.extend_from_slice(&(decompressed_size as u32).to_le_bytes()[..]);
    new_out.append(&mut out);

    let hash = cityhash_rs::cityhash_102_128(&new_out[..]);
    writer.write_u64_le((hash >> 64) as u64).await?;
    writer.write_u64_le(hash as u64).await?;
    writer.write_all(&new_out[..]).await?;

    Ok(())
}

/// Compress from pooled buffer – reduces malloc churn for high-throughput inserts.
#[expect(clippy::cast_possible_truncation)]
pub(crate) async fn compress_data_pooled<W: ClickHouseWrite>(
    writer: &mut W,
    raw: crate::simd::PooledBuffer,
    compression: CompressionMethod,
) -> Result<()> {
    let decompressed_size = raw.len();
    let mut out = match compression {
        // ZSTD with default compression level (1)
        CompressionMethod::ZSTD => zstd::bulk::compress(&raw, 1)
            .map_err(|e| Error::SerializeError(format!("ZSTD compress error: {e}")))?,
        // LZ4
        CompressionMethod::LZ4 => lz4_flex::compress(&raw),
        // None
        CompressionMethod::None => return Ok(()),
    };

    // Drop the input buffer early to return it to the pool
    drop(raw);

    let mut new_out = Vec::with_capacity(out.len() + 13);
    new_out.push(compression.byte());
    new_out.extend_from_slice(&(out.len() as u32 + 9).to_le_bytes()[..]);
    new_out.extend_from_slice(&(decompressed_size as u32).to_le_bytes()[..]);
    new_out.append(&mut out);

    let hash = cityhash_rs::cityhash_102_128(&new_out[..]);
    writer.write_u64_le((hash >> 64) as u64).await?;
    writer.write_u64_le(hash as u64).await?;
    writer.write_all(&new_out[..]).await?;

    Ok(())
}

/// Read and decompress a single chunk. Validates CityHash128 checksum.
pub(crate) async fn decompress_data_async(
    reader: &mut impl ClickHouseRead,
    compression: CompressionMethod,
) -> Result<Vec<u8>> {
    // Read checksum (16 bytes)
    let checksum_high = reader
        .read_u64_le()
        .await
        .map_err(|e| Error::Protocol(format!("Failed to read checksum high: {e}")))?;
    let checksum_low = reader
        .read_u64_le()
        .await
        .map_err(|e| Error::Protocol(format!("Failed to read checksum low: {e}")))?;
    let checksum = (u128::from(checksum_high) << 64) | u128::from(checksum_low);

    // Read compression header (9 bytes)
    let type_byte = reader
        .read_u8()
        .await
        .map_err(|e| Error::Protocol(format!("Failed to read compression type: {e}")))?;
    if type_byte != compression.byte() {
        return Err(Error::Protocol(format!(
            "Unexpected compression algorithm for {compression}: {type_byte:02x}"
        )));
    }

    let compressed_size = reader
        .read_u32_le()
        .await
        .map_err(|e| Error::Protocol(format!("Failed to read compressed size: {e}")))?;
    let decompressed_size = reader
        .read_u32_le()
        .await
        .map_err(|e| Error::Protocol(format!("Failed to read decompressed size: {e}")))?;

    // Sanity checks
    if compressed_size > 100_000_000 || decompressed_size > 1_000_000_000 {
        return Err(Error::Protocol("Chunk size too large".to_string()));
    }

    // Build the complete compressed block for checksum validation
    let mut compressed = vec![0u8; compressed_size as usize];
    let _ = reader
        .read_exact(&mut compressed[9..])
        .await
        .map_err(|e| Error::Protocol(format!("Failed to read compressed payload: {e}")))?;
    compressed[0] = type_byte;
    compressed[1..5].copy_from_slice(&compressed_size.to_le_bytes());
    compressed[5..9].copy_from_slice(&decompressed_size.to_le_bytes());

    // Validate checksum
    let calc_checksum = cityhash_rs::cityhash_102_128(&compressed);
    if calc_checksum != checksum {
        return Err(Error::Protocol(format!(
            "Checksum mismatch: expected {checksum:032x}, got {calc_checksum:032x}"
        )));
    }

    // Decompress based on compression method
    match compression {
        CompressionMethod::LZ4 => {
            lz4_flex::decompress(&compressed[9..], decompressed_size as usize)
                .map_err(|e| Error::DeserializeError(format!("LZ4 decompress error: {e}")))
        }
        CompressionMethod::ZSTD => {
            zstd::bulk::decompress(&compressed[9..], decompressed_size as usize)
                .map_err(|e| Error::DeserializeError(format!("ZSTD decompress error: {e}")))
        }
        CompressionMethod::None => {
            Err(Error::DeserializeError("Attempted to decompress uncompressed data".into()))
        }
    }
}

type BlockReadingFuture<'a, R> =
    Pin<Box<dyn Future<Output = Result<(Vec<u8>, &'a mut R)>> + Send + Sync + 'a>>;

/// Async reader that decompresses ClickHouse blocks on-the-fly.
pub(crate) struct DecompressionReader<'a, R: ClickHouseRead + 'static> {
    mode:                 CompressionMethod,
    inner:                Option<&'a mut R>,
    decompressed:         Vec<u8>,
    position:             usize,
    block_reading_future: Option<BlockReadingFuture<'a, R>>,
}

impl<'a, R: ClickHouseRead> DecompressionReader<'a, R> {
    /// Create decompressor. Reads first chunk immediately.
    pub(crate) async fn new(mode: CompressionMethod, inner: &'a mut R) -> Result<Self> {
        // Decompress intial block
        let decompressed = decompress_data_async(inner, mode).await.inspect_err(|error| {
            tracing::error!(?error, "Error decompressing data");
        })?;

        Ok(Self { mode, inner: Some(inner), decompressed, position: 0, block_reading_future: None })
    }
}

impl<R: ClickHouseRead> AsyncRead for DecompressionReader<'_, R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        // Check if we have a pending operation to complete first
        if let Some(block_reading_future) = self.block_reading_future.as_mut() {
            match block_reading_future.poll_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok((value, inner))) => {
                    drop(self.block_reading_future.take());
                    self.decompressed = value;
                    self.position = 0;
                    self.inner = Some(inner);
                    // Fall through to serve data or potentially read more
                }
                Poll::Ready(Err(e)) => {
                    drop(self.block_reading_future.take());
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e,
                    )));
                }
            }
        }

        // If we have available data, serve what we can
        let available = self.decompressed.len() - self.position;
        if available > 0 {
            let to_serve = available.min(buf.remaining());
            buf.put_slice(&self.decompressed[self.position..self.position + to_serve]);
            self.position += to_serve;
            return Poll::Ready(Ok(()));
        }

        // We have no data available in our buffer
        // Try to read the next chunk if we still have an inner reader
        if let Some(inner) = self.inner.take() {
            let mode = self.mode;
            self.block_reading_future = Some(Box::pin(async move {
                let value = decompress_data_async(inner, mode).await?;
                Ok((value, inner))
            }));
            // Immediately try to poll the future we just created
            return self.poll_read(cx, buf);
        }

        // No inner reader left AND no data in buffer - this is true EOF
        // Only return EOF if we've actually exhausted all data sources
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn test_write_compressed_data_lz4() {
        let data = b"test data for compression".to_vec();
        let mut buffer = Vec::new();

        compress_data(&mut buffer, data.clone(), CompressionMethod::LZ4).await.unwrap();
        assert!(!buffer.is_empty());
        assert!(buffer.len() >= 25); // 16 checksum + 9 header + payload

        // Verify we can decompress it back
        let mut reader = Cursor::new(buffer);
        let decompressed =
            decompress_data_async(&mut reader, CompressionMethod::LZ4).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_write_compressed_data_zstd() {
        let data = b"test data for ZSTD compression".to_vec();
        let mut buffer = Vec::new();

        compress_data(&mut buffer, data.clone(), CompressionMethod::ZSTD).await.unwrap();
        assert!(!buffer.is_empty());
        assert!(buffer.len() >= 25); // 16 checksum + 9 header + payload

        // Verify we can decompress it back
        let mut reader = Cursor::new(buffer);
        let decompressed =
            decompress_data_async(&mut reader, CompressionMethod::ZSTD).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_write_compressed_data_none() {
        let data = b"test data no compression".to_vec();
        let mut buffer = Vec::new();

        compress_data(&mut buffer, data.clone(), CompressionMethod::None).await.unwrap();
        assert!(buffer.is_empty());

        // For None compression, the data should be in the same chunk format
        let mut reader = Cursor::new(buffer);
        let decompressed = decompress_data_async(&mut reader, CompressionMethod::None).await;
        assert!(decompressed.is_err());
    }

    #[tokio::test]
    async fn test_decompress_data_lz4() {
        let data = b"test data for LZ4 decompression".to_vec();

        // First compress the data
        let mut buffer = Vec::new();
        compress_data(&mut buffer, data.clone(), CompressionMethod::LZ4).await.unwrap();

        // Then decompress it
        let mut reader = Cursor::new(buffer);
        let decompressed =
            decompress_data_async(&mut reader, CompressionMethod::LZ4).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_decompress_data_zstd() {
        let data = b"test data for ZSTD decompression".to_vec();

        // First compress the data
        let mut buffer = Vec::new();
        compress_data(&mut buffer, data.clone(), CompressionMethod::ZSTD).await.unwrap();

        // Then decompress it
        let mut reader = Cursor::new(buffer);
        let decompressed =
            decompress_data_async(&mut reader, CompressionMethod::ZSTD).await.unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn test_decompression_reader_single_chunk() {
        let data = b"test data for single chunk reading".to_vec();
        let expected_len = data.len();

        // Prepare compressed data
        let mut buffer = Vec::new();
        compress_data(&mut buffer, data.clone(), CompressionMethod::LZ4).await.unwrap();

        // Create decompression reader
        let mut reader = Cursor::new(buffer);
        let mut decompression_reader =
            DecompressionReader::new(CompressionMethod::LZ4, &mut reader).await.unwrap();

        // Read exactly the amount of data we expect (like real ClickHouse usage)
        let mut result = vec![0u8; expected_len];
        let _ = decompression_reader.read_exact(&mut result).await.unwrap();

        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_round_trip_compression() {
        let original_data = b"This is a longer piece of test data that should compress well with both LZ4 and ZSTD algorithms".to_vec();

        for compression in [CompressionMethod::LZ4, CompressionMethod::ZSTD] {
            // Compress
            let mut compressed_buffer = Vec::new();
            compress_data(&mut compressed_buffer, original_data.clone(), compression)
                .await
                .unwrap();

            // Decompress
            let mut reader = Cursor::new(compressed_buffer);
            let decompressed = decompress_data_async(&mut reader, compression).await.unwrap();

            assert_eq!(decompressed, original_data, "Round trip failed for {compression:?}");
        }
    }

    #[tokio::test]
    async fn test_checksum_validation() {
        let data = b"test data for checksum validation".to_vec();

        // Create properly compressed data
        let mut buffer = Vec::new();
        compress_data(&mut buffer, data.clone(), CompressionMethod::LZ4).await.unwrap();

        // Corrupt the checksum (first 8 bytes)
        buffer[0] ^= 0xFF;

        // Decompression should fail due to checksum mismatch
        let mut reader = Cursor::new(buffer);
        let result = decompress_data_async(&mut reader, CompressionMethod::LZ4).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Checksum mismatch"));
    }
}
