//! Counting global allocator, used to report allocations per processed frame.
//!
//! Install it in a binary with:
//! ```ignore
//! #[global_allocator]
//! static A: hft_bench::alloc::Counting = hft_bench::alloc::Counting;
//! ```
//! If it is not installed, [`count`] stays at zero.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

static ALLOCS: AtomicU64 = AtomicU64::new(0);

pub struct Counting;

#[allow(unsafe_code)]
// SAFETY: delegates every call unchanged to the system allocator; only counts.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: same contract as the caller's.
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        // SAFETY: same contract as the caller's.
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: same contract as the caller's.
        unsafe { System.realloc(p, l, n) }
    }
}

pub fn count() -> u64 {
    ALLOCS.load(Ordering::Relaxed)
}
