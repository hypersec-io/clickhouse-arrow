//! io_uring support for Linux 5.10+ with runtime detection.
//!
//! This module provides io_uring-based async I/O when available, with automatic
//! fallback to standard epoll-based I/O on unsupported systems.
//!
//! Part of HyperSec DFE optimisations ported to clickhouse-arrow for high-throughput
//! ClickHouse workloads where syscall overhead becomes significant.
//!
//! # Feature Gate
//!
//! Enable with the `io-uring` feature flag in Cargo.toml:
//! ```toml
//! clickhouse-arrow = { version = "0.4", features = ["io-uring"] }
//! ```
//!
//! # Runtime Detection
//!
//! io_uring requires:
//! - Linux kernel 5.10 or later
//! - Successful io_uring probe at runtime
//!
//! Use [`is_iouring_available`] to check availability before using io_uring APIs.

use std::sync::OnceLock;

/// Cached result of io_uring availability check.
static IOURING_AVAILABLE: OnceLock<bool> = OnceLock::new();

/// Check if io_uring is available on this system.
///
/// This function performs a one-time check for io_uring support by:
/// 1. Verifying we're on Linux
/// 2. Checking kernel version >= 5.10
/// 3. Probing the io_uring subsystem
///
/// The result is cached for subsequent calls.
///
/// # Returns
///
/// `true` if io_uring is available and usable, `false` otherwise.
///
/// # Example
///
/// ```rust
/// use clickhouse_arrow::io_uring::is_iouring_available;
///
/// if is_iouring_available() {
///     println!("io_uring is available!");
/// } else {
///     println!("Falling back to epoll");
/// }
/// ```
#[must_use]
pub fn is_iouring_available() -> bool {
    *IOURING_AVAILABLE.get_or_init(detect_iouring_support)
}

/// Perform the actual io_uring detection.
fn detect_iouring_support() -> bool {
    // Only available on Linux
    #[cfg(not(target_os = "linux"))]
    {
        return false;
    }

    #[cfg(target_os = "linux")]
    {
        // Check kernel version
        if !check_kernel_version() {
            tracing::debug!("io_uring: kernel version < 5.10, not available");
            return false;
        }

        // Try to probe io_uring
        if !probe_iouring() {
            tracing::debug!("io_uring: probe failed, not available");
            return false;
        }

        tracing::info!("io_uring: available and enabled");
        true
    }
}

/// Check if kernel version is >= 5.10.
#[cfg(target_os = "linux")]
fn check_kernel_version() -> bool {
    use std::fs;

    // Read /proc/version for kernel version
    let version = match fs::read_to_string("/proc/version") {
        Ok(v) => v,
        Err(_) => return false,
    };

    // Parse "Linux version X.Y.Z ..."
    let parts: Vec<&str> = version.split_whitespace().collect();
    if parts.len() < 3 {
        return false;
    }

    let version_str = parts[2];
    let version_parts: Vec<&str> = version_str.split('.').collect();
    if version_parts.len() < 2 {
        return false;
    }

    let major: u32 = version_parts[0].parse().unwrap_or(0);
    let minor: u32 = version_parts[1]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .unwrap_or(0);

    // Require 5.10+
    major > 5 || (major == 5 && minor >= 10)
}

/// Probe io_uring subsystem by attempting to create a minimal ring.
#[cfg(target_os = "linux")]
fn probe_iouring() -> bool {
    // Use libc to probe io_uring_setup syscall
    // This is a lightweight check that doesn't require the full tokio-uring runtime

    use std::os::raw::c_int;

    // io_uring_setup syscall number for x86_64
    #[cfg(target_arch = "x86_64")]
    const SYS_IO_URING_SETUP: i64 = 425;

    #[cfg(target_arch = "aarch64")]
    const SYS_IO_URING_SETUP: i64 = 425;

    // Minimal io_uring_params struct
    #[repr(C)]
    struct IoUringParams {
        sq_entries: u32,
        cq_entries: u32,
        flags: u32,
        sq_thread_cpu: u32,
        sq_thread_idle: u32,
        features: u32,
        wq_fd: u32,
        resv: [u32; 3],
        sq_off: [u64; 10],
        cq_off: [u64; 10],
    }

    let mut params = IoUringParams {
        sq_entries: 0,
        cq_entries: 0,
        flags: 0,
        sq_thread_cpu: 0,
        sq_thread_idle: 0,
        features: 0,
        wq_fd: 0,
        resv: [0; 3],
        sq_off: [0; 10],
        cq_off: [0; 10],
    };

    // SAFETY: We're making a syscall with valid parameters
    // A return value >= 0 means io_uring is available (returns fd)
    // A return value < 0 means error (could be ENOSYS if not supported)
    let result = unsafe {
        libc::syscall(
            SYS_IO_URING_SETUP,
            1u32, // entries
            &mut params as *mut IoUringParams,
        )
    };

    if result >= 0 {
        // Close the file descriptor we just created
        let _ = unsafe { libc::close(result as c_int) };
        true
    } else {
        false
    }
}

/// Get a human-readable description of io_uring status.
#[must_use]
pub fn iouring_status() -> &'static str {
    if is_iouring_available() {
        "io_uring: available (Linux 5.10+)"
    } else {
        #[cfg(target_os = "linux")]
        {
            "io_uring: not available (kernel < 5.10 or probe failed)"
        }
        #[cfg(not(target_os = "linux"))]
        {
            "io_uring: not available (requires Linux)"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iouring_detection_is_deterministic() {
        // Call multiple times - should return same result
        let first = is_iouring_available();
        let second = is_iouring_available();
        let third = is_iouring_available();

        assert_eq!(first, second);
        assert_eq!(second, third);
    }

    #[test]
    fn test_iouring_status_returns_string() {
        let status = iouring_status();
        assert!(!status.is_empty());
        assert!(status.starts_with("io_uring:"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_kernel_version_check() {
        // This should return true or false without panicking
        let result = check_kernel_version();
        println!("Kernel version check: {result}");
    }
}
