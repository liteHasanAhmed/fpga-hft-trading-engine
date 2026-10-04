//! Replay: turn a recording (or a hand-written text fixture) back into the exact
//! normalized event stream the live process saw, and drive deterministic
//! simulations over it.
//!
//! *Replay speed* (wall time to process) is unrelated to *simulated market time*:
//! the [`Scheduler`] advances a virtual clock event by event, as fast as the CPU allows.

mod items;
mod scheduler;
pub mod snapcheck;
mod verify;

pub use items::{
    Decoder, FixtureSource, Item, ItemStream, RecordingSource, Venue, detect_venue, load_raw_records, open_source,
};
pub use scheduler::Scheduler;
pub use snapcheck::{SnapOutcome, SnapshotVerifier};
pub use verify::{VerifyReport, verify_stream};
