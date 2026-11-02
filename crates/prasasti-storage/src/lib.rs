#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use prasasti_memtable::ColumnarMemTable;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupted segment header or checksum mismatch")]
    CorruptedSegment,
    #[error("not implemented")]
    NotImplemented,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentZoneMap {
    pub segment_id: u64,
    pub path: PathBuf,
    pub min_ts: i64,
    pub max_ts: i64,
    pub row_count: usize,
    pub payload_crc32: u32,
}

impl SegmentZoneMap {
    pub fn overlaps(&self, start_ts: i64, end_ts: i64) -> bool {
        self.min_ts <= end_ts && self.max_ts >= start_ts
    }
}

/// Open-Core Enterprise Extension Hook (REQ-PRS-06):
/// Notifies enterprise storage tiers (S3/GCS cold object tier or cluster replication)
/// whenever an immutable `.prs` segment is sealed to disk.
pub trait StorageTierHook: Send + Sync {
    fn on_segment_flushed(&self, zone_map: &SegmentZoneMap);
}

pub struct SegmentStorage;

impl SegmentStorage {
    pub fn write_segment(
        _dir: impl AsRef<Path>,
        _segment_id: u64,
        _table: &ColumnarMemTable,
    ) -> Result<SegmentZoneMap, StorageError> {
        Err(StorageError::NotImplemented)
    }

    pub fn read_segment(_path: impl AsRef<Path>) -> Result<ColumnarMemTable, StorageError> {
        Err(StorageError::NotImplemented)
    }
}
