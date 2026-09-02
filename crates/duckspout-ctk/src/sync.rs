//! The sync-primitive seam for loom exploration (§8.3).
//!
//! With the `loom` feature enabled, the doubles' own atomics and mutexes
//! are loom's checked types, so `tests/loom.rs` explores every
//! memory-model interleaving of the **real** code — never a parallel copy
//! whose own drift could mask a bug. Without the feature these are exactly
//! `std::sync`'s types, and nothing else changes.

#[cfg(feature = "loom")]
pub(crate) use loom::sync::{Mutex, atomic};
#[cfg(not(feature = "loom"))]
pub(crate) use std::sync::{Mutex, atomic};
