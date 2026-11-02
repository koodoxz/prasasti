#![forbid(unsafe_code)]

use std::path::Path;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const WAL_RECORD_BYTES: usize = 36;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("crc32 mismatch at record offset {offset}: expected {expected:#010x}, got {actual:#010x}")]
    CrcMismatch {
        offset: u64,
        expected: u32,
        actual: u32,
    },
    #[error("not implemented yet")]
    NotImplemented,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WalRecord {
    pub timestamp_ns: i64,
    pub symbol_id: u32,
    pub value: f64,
    pub weight: f64,
    pub flags: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryOutcome {
    pub records: Vec<WalRecord>,
    pub truncated_tail_bytes: u64,
}

pub struct WriteAheadLog;

impl WriteAheadLog {
    pub fn open(_path: impl AsRef<Path>) -> Result<Self, WalError> {
        Err(WalError::NotImplemented)
    }

    pub fn append(&mut self, _record: &WalRecord) -> Result<(), WalError> {
        Err(WalError::NotImplemented)
    }

    pub fn append_batch(&mut self, _records: &[WalRecord]) -> Result<(), WalError> {
        Err(WalError::NotImplemented)
    }

    pub fn sync(&mut self) -> Result<(), WalError> {
        Err(WalError::NotImplemented)
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<RecoveryOutcome, WalError> {
        Err(WalError::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn req_prs_01_wal_roundtrip_and_corrupted_tail_recovery() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("events.wal");

        let mut wal = WriteAheadLog::open(&wal_path).expect("should open WAL");
        let r1 = WalRecord {
            timestamp_ns: 1_700_000_000_000,
            symbol_id: 1,
            value: 2345.50,
            weight: 10.0,
            flags: 0,
        };
        let r2 = WalRecord {
            timestamp_ns: 1_700_000_001_000,
            symbol_id: 1,
            value: 2346.25,
            weight: 5.0,
            flags: 1,
        };
        wal.append(&r1).unwrap();
        wal.append(&r2).unwrap();
        wal.sync().unwrap();
        drop(wal);

        // Inject 15 bytes of corrupted tail simulating sudden power loss mid-write
        {
            let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
            f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
                .unwrap();
            f.sync_all().unwrap();
        }

        let outcome = WriteAheadLog::recover(&wal_path).expect("recovery must succeed");
        assert_eq!(outcome.records.len(), 2);
        assert_eq!(outcome.records[0], r1);
        assert_eq!(outcome.records[1], r2);
        assert_eq!(outcome.truncated_tail_bytes, 15);
    }
}
