#![forbid(unsafe_code)]

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const WAL_RECORD_BYTES: usize = 36;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "crc32 mismatch at record offset {offset}: expected {expected:#010x}, got {actual:#010x}"
    )]
    CrcMismatch {
        offset: u64,
        expected: u32,
        actual: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WalRecord {
    pub timestamp_ns: i64,
    pub symbol_id: u32,
    pub value: f64,
    pub weight: f64,
    pub flags: u32,
}

impl WalRecord {
    #[inline]
    pub fn encode(&self) -> [u8; WAL_RECORD_BYTES] {
        let mut buf = [0u8; WAL_RECORD_BYTES];
        buf[4..12].copy_from_slice(&self.timestamp_ns.to_le_bytes());
        buf[12..16].copy_from_slice(&self.symbol_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.value.to_bits().to_le_bytes());
        buf[24..32].copy_from_slice(&self.weight.to_bits().to_le_bytes());
        buf[32..36].copy_from_slice(&self.flags.to_le_bytes());
        let crc = crc32fast::hash(&buf[4..WAL_RECORD_BYTES]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    #[inline]
    pub fn decode(buf: &[u8; WAL_RECORD_BYTES], offset: u64) -> Result<Self, WalError> {
        let expected = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let actual = crc32fast::hash(&buf[4..WAL_RECORD_BYTES]);
        if expected != actual {
            return Err(WalError::CrcMismatch {
                offset,
                expected,
                actual,
            });
        }
        let timestamp_ns = i64::from_le_bytes(buf[4..12].try_into().unwrap());
        let symbol_id = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let value = f64::from_bits(u64::from_le_bytes(buf[16..24].try_into().unwrap()));
        let weight = f64::from_bits(u64::from_le_bytes(buf[24..32].try_into().unwrap()));
        let flags = u32::from_le_bytes(buf[32..36].try_into().unwrap());
        Ok(Self {
            timestamp_ns,
            symbol_id,
            value,
            weight,
            flags,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryOutcome {
    pub records: Vec<WalRecord>,
    pub truncated_tail_bytes: u64,
}

pub struct WriteAheadLog {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl WriteAheadLog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let path_buf = path.as_ref().to_path_buf();
        if path_buf.exists() {
            let _ = Self::recover(&path_buf)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path_buf)?;
        Ok(Self {
            path: path_buf,
            writer: BufWriter::with_capacity(64 * 1024, file),
        })
    }

    #[inline]
    pub fn append(&mut self, record: &WalRecord) -> Result<(), WalError> {
        let encoded = record.encode();
        self.writer.write_all(&encoded)?;
        Ok(())
    }

    pub fn append_batch(&mut self, records: &[WalRecord]) -> Result<(), WalError> {
        for rec in records {
            let encoded = rec.encode();
            self.writer.write_all(&encoded)?;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), WalError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), WalError> {
        self.writer.flush()?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        file.sync_data()?;
        let append_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.writer = BufWriter::with_capacity(64 * 1024, append_file);
        Ok(())
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<RecoveryOutcome, WalError> {
        let path_ref = path.as_ref();
        if !path_ref.exists() {
            return Ok(RecoveryOutcome {
                records: Vec::new(),
                truncated_tail_bytes: 0,
            });
        }

        let mut bytes = Vec::new();
        {
            let mut f = File::open(path_ref)?;
            f.read_to_end(&mut bytes)?;
        }

        let total_len = bytes.len() as u64;
        let mut offset = 0usize;
        let mut records = Vec::with_capacity(bytes.len() / WAL_RECORD_BYTES);

        while offset + WAL_RECORD_BYTES <= bytes.len() {
            let chunk: &[u8; WAL_RECORD_BYTES] =
                bytes[offset..offset + WAL_RECORD_BYTES].try_into().unwrap();
            match WalRecord::decode(chunk, offset as u64) {
                Ok(rec) => {
                    records.push(rec);
                    offset += WAL_RECORD_BYTES;
                }
                Err(WalError::CrcMismatch { .. }) => {
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        let valid_len = offset as u64;
        let truncated_tail_bytes = total_len.saturating_sub(valid_len);

        if truncated_tail_bytes > 0 {
            let file = OpenOptions::new().write(true).open(path_ref)?;
            file.set_len(valid_len)?;
            file.sync_all()?;
        }

        Ok(RecoveryOutcome {
            records,
            truncated_tail_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

        // Also test corrupting the CRC of a full 36-byte trailing frame
        {
            let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
            let mut bad_frame = r1.encode();
            bad_frame[0] ^= 0xFF; // flip CRC bits
            f.write_all(&bad_frame).unwrap();
            f.sync_all().unwrap();
        }

        let outcome2 = WriteAheadLog::recover(&wal_path).expect("corrupted frame recovery");
        assert_eq!(outcome2.records.len(), 2);
        assert_eq!(outcome2.truncated_tail_bytes, WAL_RECORD_BYTES as u64);
    }
}
