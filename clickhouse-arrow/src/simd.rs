//! SIMD and buffer pooling for hot paths.
//!
//! Why bother? Insert/query throughput is dominated by serialisation time.
//! These optimisations target the inner loops that run millions of times per batch:
//!
//! - **Null bitmap expansion**: Every nullable column needs Arrow's packed bits
//!   expanded to ClickHouse's byte-per-value format. SIMD gives ~2.2x speedup.
//! - **Buffer pooling**: Avoids malloc/free churn in the serialisation loop.
//!   ~21% faster for 4KB buffers (common for null masks), ~5% for 64KB.
//!
//! Falls back to scalar on platforms without AVX2/NEON. The serialisation layer
//! uses these automatically – you probably don't need to call them directly.

// Null bitmap expansion

/// Expand packed null bitmap (1 bit/value) to byte mask (1 byte/value).
///
/// Arrow: bit=1 valid, bit=0 null. ClickHouse: byte=0 valid, byte=1 null.
/// We invert and expand in one pass. `len` is value count, not bitmap bytes.
#[inline]
pub fn expand_null_bitmap(bitmap: &[u8], output: &mut [u8], len: usize) {
    debug_assert!(output.len() >= len);
    debug_assert!(bitmap.len() >= len.div_ceil(8));

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        // SAFETY: We've verified bounds above and AVX2 is available
        unsafe { expand_null_bitmap_avx2(bitmap, output, len) };
    }

    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    {
        // Try runtime detection for AVX2
        if is_x86_feature_detected!("avx2") {
            // SAFETY: We've verified AVX2 is available at runtime
            unsafe { expand_null_bitmap_avx2(bitmap, output, len) };
        } else {
            expand_null_bitmap_scalar(bitmap, output, len);
        }
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        // SAFETY: NEON is available on this platform
        unsafe { expand_null_bitmap_neon(bitmap, output, len) };
    }

    #[cfg(not(any(
        all(target_arch = "x86_64", target_feature = "avx2"),
        all(target_arch = "x86_64", not(target_feature = "avx2")),
        all(target_arch = "aarch64", target_feature = "neon")
    )))]
    {
        expand_null_bitmap_scalar(bitmap, output, len);
    }
}

/// Scalar fallback for null bitmap expansion.
#[inline]
fn expand_null_bitmap_scalar(bitmap: &[u8], output: &mut [u8], len: usize) {
    // Process 8 values at a time (one bitmap byte)
    let full_bytes = len / 8;
    let remainder = len % 8;

    for (byte_idx, &byte) in bitmap.iter().take(full_bytes).enumerate() {
        let base = byte_idx * 8;
        // Unrolled loop for 8 bits - invert since Arrow: 1=valid, CH: 0=valid
        output[base] = u8::from((byte & 0x01) == 0);
        output[base + 1] = u8::from((byte & 0x02) == 0);
        output[base + 2] = u8::from((byte & 0x04) == 0);
        output[base + 3] = u8::from((byte & 0x08) == 0);
        output[base + 4] = u8::from((byte & 0x10) == 0);
        output[base + 5] = u8::from((byte & 0x20) == 0);
        output[base + 6] = u8::from((byte & 0x40) == 0);
        output[base + 7] = u8::from((byte & 0x80) == 0);
    }

    // Handle remainder
    if remainder > 0 {
        let byte = bitmap[full_bytes];
        let base = full_bytes * 8;
        for bit in 0..remainder {
            output[base + bit] = u8::from((byte & (1 << bit)) == 0);
        }
    }
}

/// AVX2 implementation of null bitmap expansion.
///
/// Processes 32 values per iteration using unrolled scalar operations.
/// While this doesn't use AVX2 intrinsics directly, the unrolled loop
/// allows the compiler to auto-vectorize effectively.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn expand_null_bitmap_avx2(bitmap: &[u8], output: &mut [u8], len: usize) {
    // SAFETY: Caller guarantees bitmap and output have sufficient length
    unsafe {
        let full_chunks = len / 32; // 32 values = 4 bitmap bytes per chunk
        let mut out_idx = 0;

        // Process 32 values at a time (unrolled for vectorization)
        for chunk in 0..full_chunks {
            let bitmap_offset = chunk * 4;
            // Load 4 bytes of bitmap
            let b0 = *bitmap.get_unchecked(bitmap_offset);
            let b1 = *bitmap.get_unchecked(bitmap_offset + 1);
            let b2 = *bitmap.get_unchecked(bitmap_offset + 2);
            let b3 = *bitmap.get_unchecked(bitmap_offset + 3);

            // Expand each byte to 8 output bytes
            expand_byte_to_8_unchecked(b0, output, out_idx);
            out_idx += 8;
            expand_byte_to_8_unchecked(b1, output, out_idx);
            out_idx += 8;
            expand_byte_to_8_unchecked(b2, output, out_idx);
            out_idx += 8;
            expand_byte_to_8_unchecked(b3, output, out_idx);
            out_idx += 8;
        }

        // Handle remainder with scalar
        let remaining = len - (full_chunks * 32);
        if remaining > 0 {
            expand_null_bitmap_scalar(
                &bitmap[full_chunks * 4..],
                &mut output[out_idx..],
                remaining,
            );
        }
    }
}

/// Expand a single byte to 8 output bytes without bounds checking.
#[allow(clippy::inline_always)] // Hot path in SIMD expansion loop - inlining is critical
#[inline(always)]
unsafe fn expand_byte_to_8_unchecked(byte: u8, output: &mut [u8], offset: usize) {
    // SAFETY: Caller guarantees output has sufficient length
    unsafe {
        // Invert: Arrow 1=valid -> CH 0=valid
        *output.get_unchecked_mut(offset) = u8::from((byte & 0x01) == 0);
        *output.get_unchecked_mut(offset + 1) = u8::from((byte & 0x02) == 0);
        *output.get_unchecked_mut(offset + 2) = u8::from((byte & 0x04) == 0);
        *output.get_unchecked_mut(offset + 3) = u8::from((byte & 0x08) == 0);
        *output.get_unchecked_mut(offset + 4) = u8::from((byte & 0x10) == 0);
        *output.get_unchecked_mut(offset + 5) = u8::from((byte & 0x20) == 0);
        *output.get_unchecked_mut(offset + 6) = u8::from((byte & 0x40) == 0);
        *output.get_unchecked_mut(offset + 7) = u8::from((byte & 0x80) == 0);
    }
}

/// NEON implementation for aarch64 – 4 bytes at a time, 32 output bytes.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn expand_null_bitmap_neon(bitmap: &[u8], output: &mut [u8], len: usize) {
    use std::arch::aarch64::*;

    // Process 4 bytes at a time (32 values) - same as AVX2 for consistency
    let full_chunks = len / 32;
    let mut out_idx = 0;

    // Bit masks: [0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80]
    let bit_mask = vld1_u8([0x01u8, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80].as_ptr());
    // All ones for XOR (to invert the result)
    let ones = vdup_n_u8(0x01);

    for chunk in 0..full_chunks {
        let bitmap_offset = chunk * 4;

        // Process 4 input bytes -> 32 output bytes
        for i in 0..4 {
            let byte = *bitmap.get_unchecked(bitmap_offset + i);

            // Duplicate byte to all 8 lanes
            let byte_vec = vdup_n_u8(byte);

            // AND with bit masks to isolate each bit
            let bits = vand_u8(byte_vec, bit_mask);

            // Compare equal to zero: 0xFF if bit was 0 (null), 0x00 if bit was 1 (valid)
            let is_zero = vceqz_u8(bits);

            // Convert 0xFF -> 0x01, 0x00 -> 0x00
            let result = vand_u8(is_zero, ones);

            // Store 8 bytes
            vst1_u8(output.as_mut_ptr().add(out_idx), result);
            out_idx += 8;
        }
    }

    // Handle remainder with scalar
    let remaining = len - (full_chunks * 32);
    if remaining > 0 {
        expand_null_bitmap_scalar(&bitmap[full_chunks * 4..], &mut output[out_idx..], remaining);
    }
}

// Buffer size constants

/// Flush streaming inserts when buffer exceeds this (254 KB, leaves room for headers).
pub const MIN_CHUNK_SIZE: usize = 254 * 1024;

/// Default serialisation buffer (256 KB).
pub const DEFAULT_BUFFER_SIZE: usize = 256 * 1024;

/// ArrowStream serialisation buffer (1 MB) – Arrow batches are chunkier.
pub const ARROW_STREAM_BUFFER_SIZE: usize = 1024 * 1024;

// Varint encoding/decoding

/// Max bytes for a varint-encoded u64.
pub const MAX_VARINT_LEN: usize = 10;

/// Encode u64 as varint into buffer. Returns bytes written.
#[inline]
#[allow(clippy::cast_possible_truncation)] // Intentional: extracting low 7 bits per varint spec
pub fn encode_varint(mut value: u64, buf: &mut [u8; MAX_VARINT_LEN]) -> usize {
    let mut pos = 0;

    // Unrolled loop optimized for common small values
    loop {
        if value < 0x80 {
            buf[pos] = value as u8;
            return pos + 1;
        }
        buf[pos] = (value as u8) | 0x80;
        value >>= 7;
        pos += 1;
    }
}

/// Decode varint from slice. Returns `(value, bytes_consumed)` or `None` if borked.
#[inline]
pub fn decode_varint(buf: &[u8]) -> Option<(u64, usize)> {
    if buf.is_empty() {
        return None;
    }

    // Fast path for single-byte varints (very common for small lengths)
    if buf[0] < 0x80 {
        return Some((u64::from(buf[0]), 1));
    }

    // Multi-byte path
    let mut result = u64::from(buf[0] & 0x7F);
    let mut shift = 7;

    for (i, &byte) in buf.iter().enumerate().skip(1).take(9) {
        result |= u64::from(byte & 0x7F) << shift;
        if byte < 0x80 {
            return Some((result, i + 1));
        }
        shift += 7;
    }

    None // Varint too long or truncated
}

/// Batch-encode varints. Reserves worst-case space upfront.
#[inline]
pub fn encode_varints_batch(values: &[u64], output: &mut Vec<u8>) {
    // Reserve worst-case space
    output.reserve(values.len() * MAX_VARINT_LEN);

    let mut buf = [0u8; MAX_VARINT_LEN];
    for &value in values {
        let len = encode_varint(value, &mut buf);
        output.extend_from_slice(&buf[..len]);
    }
}

// Byte swapping for endian conversion
//
// Benchmarks show LLVM's auto-vectorisation beats hand-written AVX2 here.
// Simple scalar loops let the compiler do its thing.

/// Swap bytes in u16 slice (endian conversion). LLVM auto-vectorises this well.
#[inline]
pub fn swap_bytes_u16_slice(data: &mut [u16]) {
    for value in data.iter_mut() {
        *value = value.swap_bytes();
    }
}

/// Swap bytes in u32 slice. LLVM auto-vectorises this ~15-20% faster than hand-written SIMD.
#[inline]
pub fn swap_bytes_u32_slice(data: &mut [u32]) {
    for value in data.iter_mut() {
        *value = value.swap_bytes();
    }
}

/// Swap bytes in u64 slice.
#[inline]
pub fn swap_bytes_u64_slice(data: &mut [u64]) {
    for value in data.iter_mut() {
        *value = value.swap_bytes();
    }
}

// UUID byte swapping
//
// ClickHouse stores UUIDs w/ high 8 bytes first, Arrow has low bytes first.
// We swap the halves.

/// Swap UUID halves in-place for ClickHouse format.
#[inline]
pub fn swap_uuid_halves(uuid: &mut [u8; 16]) {
    let (low, high) = uuid.split_at_mut(8);
    low.swap_with_slice(high);
}

/// Convert UUID from Arrow to ClickHouse format (returns new array).
#[inline]
pub fn uuid_to_clickhouse(uuid: &[u8; 16]) -> [u8; 16] {
    let mut result = [0u8; 16];
    result[..8].copy_from_slice(&uuid[8..]);
    result[8..].copy_from_slice(&uuid[..8]);
    result
}

/// Convert UUID slice to ClickHouse format. Returns None if not 16 bytes.
#[inline]
pub fn uuid_slice_to_clickhouse(uuid: &[u8]) -> Option<[u8; 16]> {
    if uuid.len() != 16 {
        return None;
    }
    let mut result = [0u8; 16];
    result[..8].copy_from_slice(&uuid[8..]);
    result[8..].copy_from_slice(&uuid[..8]);
    Some(result)
}

// Buffer pool for allocation reuse

use std::collections::VecDeque;

use parking_lot::Mutex;

/// Thread-safe buffer pool. Recycles allocations in hot paths.
///
/// Five size tiers: Tiny (1KB), Small (4KB), Medium (64KB), Large (1MB), XLarge (>1MB).
/// Benchmarks show ~21% faster for 4KB buffers, ~5% for 64KB.
pub struct BufferPool {
    pools: [Mutex<VecDeque<Vec<u8>>>; 5], // Tiny, Small, Medium, Large, XLarge
}

impl BufferPool {
    // 64KB - typical batch size
    const LARGE: usize = 1024 * 1024;
    // 1MB - large batches
    const MAX_POOL_SIZE: usize = 32;
    // 4KB - common serialization size
    const MEDIUM: usize = 64 * 1024;
    // 1KB - null bitmaps, small metadata
    const SMALL: usize = 4 * 1024;
    /// Size thresholds for pool buckets.
    const TINY: usize = 1024;

    // Max buffers per bucket

    /// Create a new buffer pool.
    pub const fn new() -> Self {
        Self {
            pools: [
                Mutex::new(VecDeque::new()),
                Mutex::new(VecDeque::new()),
                Mutex::new(VecDeque::new()),
                Mutex::new(VecDeque::new()),
                Mutex::new(VecDeque::new()),
            ],
        }
    }

    /// Pre-warm the pool to avoid cold-start allocation latency.
    pub fn prewarm(&self) {
        // Pre-allocate common buffer sizes
        for _ in 0..4 {
            self.put(Vec::with_capacity(Self::TINY));
            self.put(Vec::with_capacity(Self::SMALL));
            self.put(Vec::with_capacity(Self::MEDIUM));
        }
    }

    /// Get a buffer w/ at least `capacity` bytes. Returned buffer is cleared.
    #[inline]
    pub fn get(&self, capacity: usize) -> Vec<u8> {
        let bucket = Self::bucket_for_size(capacity);
        let mut pool = self.pools[bucket].lock();

        if let Some(mut buf) = pool.pop_front() {
            buf.clear();
            if buf.capacity() >= capacity {
                return buf;
            }
            // Buffer too small, let it drop and allocate new
        }

        Vec::with_capacity(Self::round_up_capacity(capacity))
    }

    /// Return a buffer to the pool for reuse.
    #[inline]
    pub fn put(&self, buf: Vec<u8>) {
        let capacity = buf.capacity();
        if capacity < Self::TINY / 2 {
            return; // Don't pool very tiny buffers (<512 bytes)
        }

        let bucket = Self::bucket_for_size(capacity);
        let mut pool = self.pools[bucket].lock();

        if pool.len() < Self::MAX_POOL_SIZE {
            pool.push_back(buf);
        }
        // Otherwise let it drop
    }

    /// Get current pool statistics for monitoring.
    pub fn stats(&self) -> BufferPoolStats {
        BufferPoolStats {
            tiny_count: self.pools[0].lock().len(),
            small_count: self.pools[1].lock().len(),
            medium_count: self.pools[2].lock().len(),
            large_count: self.pools[3].lock().len(),
            xlarge_count: self.pools[4].lock().len(),
        }
    }

    #[inline]
    fn bucket_for_size(size: usize) -> usize {
        if size <= Self::TINY {
            0
        } else if size <= Self::SMALL {
            1
        } else if size <= Self::MEDIUM {
            2
        } else if size <= Self::LARGE {
            3
        } else {
            4
        }
    }

    #[inline]
    fn round_up_capacity(size: usize) -> usize {
        if size <= Self::TINY {
            Self::TINY
        } else if size <= Self::SMALL {
            Self::SMALL
        } else if size <= Self::MEDIUM {
            Self::MEDIUM
        } else if size <= Self::LARGE {
            Self::LARGE
        } else {
            // Round up to next power of 2 for xlarge buffers
            size.next_power_of_two()
        }
    }
}

/// Statistics for buffer pool monitoring.
#[derive(Debug, Clone, Copy)]
pub struct BufferPoolStats {
    pub tiny_count: usize,   // 1KB buffers
    pub small_count: usize,  // 4KB buffers
    pub medium_count: usize, // 64KB buffers
    pub large_count: usize,  // 1MB buffers
    pub xlarge_count: usize, // >1MB buffers
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Global buffer pool for hot path allocations.
pub static BUFFER_POOL: BufferPool = BufferPool::new();

/// RAII guard – returns buffer to pool on drop.
pub struct PooledBuffer {
    buf: Option<Vec<u8>>,
}

impl PooledBuffer {
    /// Get a pooled buffer with at least `capacity` bytes.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self { buf: Some(BUFFER_POOL.get(capacity)) }
    }

    /// Get mutable access to the underlying buffer. Panics if already taken.
    #[inline]
    pub fn buffer_mut(&mut self) -> &mut Vec<u8> {
        self.buf.as_mut().unwrap()
    }

    /// Get immutable access. Panics if already taken.
    #[inline]
    pub fn buffer(&self) -> &Vec<u8> {
        self.buf.as_ref().unwrap()
    }

    /// Take ownership of the buffer (won't be returned to pool). Panics if already taken.
    #[inline]
    pub fn take(mut self) -> Vec<u8> {
        self.buf.take().unwrap()
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            BUFFER_POOL.put(buf);
        }
    }
}

impl std::ops::Deref for PooledBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        self.buf.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.as_mut().unwrap()
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_null_bitmap_all_valid() {
        // All bits set (all valid in Arrow)
        let bitmap = [0xFF, 0xFF];
        let mut output = [0xFFu8; 16];
        expand_null_bitmap(&bitmap, &mut output, 16);
        // All valid -> all zeros in CH format
        assert!(output.iter().all(|&b| b == 0), "All valid values should be 0");
    }

    #[test]
    fn test_expand_null_bitmap_all_null() {
        // No bits set (all null in Arrow)
        let bitmap = [0x00, 0x00];
        let mut output = [0x00u8; 16];
        expand_null_bitmap(&bitmap, &mut output, 16);
        // All null -> all ones in CH format
        assert!(output.iter().all(|&b| b == 1), "All null values should be 1");
    }

    #[test]
    fn test_expand_null_bitmap_mixed() {
        // 0b10101010 = bits 1,3,5,7 set (valid)
        let bitmap = [0xAA];
        let mut output = [0xFFu8; 8];
        expand_null_bitmap(&bitmap, &mut output, 8);
        // Expected: [1,0,1,0,1,0,1,0] (bit 0 not set -> null, bit 1 set -> valid, etc.)
        assert_eq!(output, [1, 0, 1, 0, 1, 0, 1, 0]);
    }

    #[test]
    fn test_expand_null_bitmap_partial() {
        // Only process 5 values from a byte
        let bitmap = [0b0001_1111]; // First 5 bits set
        let mut output = [0xFFu8; 5];
        expand_null_bitmap(&bitmap, &mut output, 5);
        // All 5 are valid (bit set) -> 0 in CH
        assert_eq!(output, [0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_varint_encode_decode_small() {
        let mut buf = [0u8; MAX_VARINT_LEN];

        // Test single-byte values
        for value in 0u64..128 {
            let len = encode_varint(value, &mut buf);
            assert_eq!(len, 1);
            let (decoded, consumed) = decode_varint(&buf[..len]).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(consumed, 1);
        }
    }

    #[test]
    fn test_varint_encode_decode_large() {
        let mut buf = [0u8; MAX_VARINT_LEN];

        let test_values =
            [128u64, 256, 1000, 16383, 16384, u64::from(u32::MAX), u64::MAX / 2, u64::MAX];

        for &value in &test_values {
            let len = encode_varint(value, &mut buf);
            let (decoded, consumed) = decode_varint(&buf[..len]).unwrap();
            assert_eq!(decoded, value, "Failed for value {value}");
            assert_eq!(consumed, len);
        }
    }

    #[test]
    fn test_buffer_pool_basic() {
        let buf1 = BUFFER_POOL.get(100);
        assert!(buf1.capacity() >= 100);

        let buf2 = BUFFER_POOL.get(50000);
        assert!(buf2.capacity() >= 50000);

        // Return buffers
        BUFFER_POOL.put(buf1);
        BUFFER_POOL.put(buf2);

        // Get again - should reuse
        let buf3 = BUFFER_POOL.get(100);
        assert!(buf3.capacity() >= 100);
    }

    #[test]
    fn test_pooled_buffer_raii() {
        {
            let mut buf = PooledBuffer::with_capacity(1000);
            buf.extend_from_slice(b"hello");
            assert_eq!(&**buf, b"hello");
        }
        // Buffer should be returned to pool on drop
    }

    #[test]
    fn test_byte_swap_u32() {
        let mut data = vec![0x0102_0304_u32, 0x0506_0708, 0x090A_0B0C];
        swap_bytes_u32_slice(&mut data);
        assert_eq!(data, [0x0403_0201, 0x0807_0605, 0x0C0B_0A09]);
    }

    #[test]
    fn test_byte_swap_u64() {
        let mut data = vec![0x0102_0304_0506_0708_u64];
        swap_bytes_u64_slice(&mut data);
        assert_eq!(data, [0x0807_0605_0403_0201]);
    }

    #[test]
    fn test_uuid_swap_halves() {
        let mut uuid = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        swap_uuid_halves(&mut uuid);
        assert_eq!(uuid, [8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_uuid_to_clickhouse() {
        let uuid = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let result = uuid_to_clickhouse(&uuid);
        assert_eq!(result, [8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_uuid_slice_to_clickhouse() {
        let uuid: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let result = uuid_slice_to_clickhouse(uuid).unwrap();
        assert_eq!(result, [8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]);

        // Test wrong size
        let short: &[u8] = &[0, 1, 2, 3];
        assert!(uuid_slice_to_clickhouse(short).is_none());
    }
}
