#![forbid(unsafe_code)]

use std::path::Path;
use std::sync::Arc;
use prasasti_storage::{SegmentZoneMap, StorageError, StorageTierHook};
use prasasti_wal::WalError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PrasastiError {
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("symbol not found: {0}")]
    SymbolNotFound(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregationSummary {
    pub count: usize,
    pub min: f64,
    pub max: f64,
    pub sum: f64,
    pub mean: f64,
    pub vwap: f64,
    pub total_weight: f64,
    pub scanned_segments: usize,
    pub skipped_segments: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OhlcvBucket {
    pub bucket_start_ns: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub vwap: f64,
    pub count: usize,
}

pub struct PrasastiEngine;

impl PrasastiEngine {
    pub fn open(_data_dir: impl AsRef<Path>, _max_memtable_rows: usize) -> Result<Self, PrasastiError> {
        Err(PrasastiError::InvalidConfig("Not implemented".into()))
    }

    pub fn with_storage_tier_hook(self, _hook: Arc<dyn StorageTierHook>) -> Self {
        self
    }

    pub fn append(
        &mut self,
        _symbol: &str,
        _timestamp_ns: i64,
        _value: f64,
        _weight: f64,
        _flags: u32,
    ) -> Result<(), PrasastiError> {
        Err(PrasastiError::InvalidConfig("Not implemented".into()))
    }

    pub fn flush_memtable(&mut self) -> Result<Option<SegmentZoneMap>, PrasastiError> {
        Err(PrasastiError::InvalidConfig("Not implemented".into()))
    }

    pub fn aggregate_range(
        &self,
        _symbol: Option<&str>,
        _start_ts: i64,
        _end_ts: i64,
    ) -> Result<AggregationSummary, PrasastiError> {
        Err(PrasastiError::InvalidConfig("Not implemented".into()))
    }

    pub fn ohlcv_buckets(
        &self,
        _symbol: &str,
        _start_ts: i64,
        _end_ts: i64,
        _interval_ns: i64,
    ) -> Result<Vec<OhlcvBucket>, PrasastiError> {
        Err(PrasastiError::InvalidConfig("Not implemented".into()))
    }

    pub fn extract_timesfm_patches(
        &self,
        _symbol: &str,
        _start_ts: i64,
        _end_ts: i64,
        _patch_len: usize,
    ) -> Result<Vec<Vec<f64>>, PrasastiError> {
        Err(PrasastiError::InvalidConfig("Not implemented".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn req_prs_03_04_05_06_end_to_end_engine_aggregation_ohlcv_and_pruning() {
        let dir = tempdir().unwrap();
        let mut engine = PrasastiEngine::open(dir.path(), 2).expect("engine must open");
        engine.append("XAUUSD", 1_000, 2000.0, 1.0, 0).unwrap();
        engine.append("XAUUSD", 2_000, 2010.0, 3.0, 0).unwrap();
        let summary = engine.aggregate_range(Some("XAUUSD"), 1_000, 2_000).unwrap();
        assert_eq!(summary.count, 2);
        assert!((summary.vwap - 2007.5).abs() < 1e-9);
    }
}
