//! The application's log of a key's generation 1, held whole by every node that may originate the key.
use axon_stream::{Cursor, ReadBounds, RecoveryError, RecoverySource, ReplayRange, SequencedBatch, StreamKey};
use bytes::Bytes;

/// Record `s` is the number `s - self.0` in 8 big-endian bytes, as the test publishes it.
pub struct Numbered(pub u64);

impl RecoverySource for Numbered {
    fn available_range(&self, _: &StreamKey) -> Result<ReplayRange, RecoveryError> {
        Ok(ReplayRange { generation: 1, first: 1, last: u64::MAX - 1 })
    }

    fn read_after(
        &self,
        _: &StreamKey,
        after: Cursor,
        through: u64,
        bounds: ReadBounds,
    ) -> Result<Vec<SequencedBatch>, RecoveryError> {
        let last = through.min(after.sequence + bounds.max_records.min(bounds.max_bytes / 8) as u64);
        let record = |s: u64| Bytes::copy_from_slice(&(s - self.0).to_be_bytes());
        let records: Vec<Bytes> = (after.sequence + 1..=last).map(record).collect();
        if records.is_empty() {
            return Err(RecoveryError::LimitExceeded);
        }
        Ok(vec![SequencedBatch::new(1, after.sequence + 1, records)?])
    }
}
