// Project:   py-clickhouse-arrow
// File:      runtime.rs
// Purpose:   Tokio runtime management for sync Python API
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HyperSec

//! Tokio runtime management for bridging async Rust to sync Python.
//!
//! Creates a lazily-initialised multi-threaded Tokio runtime that persists
//! for the lifetime of the Python module. Provides `block_on()` for executing
//! async code synchronously from Python.

use std::future::Future;

use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

/// Global Tokio runtime for executing async operations.
///
/// Lazily initialised on first use, persists for module lifetime.
/// Uses a multi-threaded scheduler with 4 worker threads.
static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .thread_name("clickhouse-arrow-py")
        .build()
        .expect("failed to create Tokio runtime")
});

/// Execute an async future synchronously, blocking until completion.
///
/// This is the primary bridge between async Rust code and sync Python calls.
/// Uses the global runtime to execute the future.
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    RUNTIME.block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_on_basic() {
        let result = block_on(async { 42 });
        assert_eq!(result, 42);
    }

    #[test]
    fn test_block_on_async_sleep() {
        let start = std::time::Instant::now();
        block_on(async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        });
        assert!(start.elapsed() >= std::time::Duration::from_millis(10));
    }
}
