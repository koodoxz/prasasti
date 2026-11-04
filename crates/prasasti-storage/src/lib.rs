#![forbid(unsafe_code)]

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use prasasti_memtable::ColumnarMemTable;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const PRS_MAGIC: [u8; 4] = *b"PRS1";
pub const PRS_HEADER_BYTES: usize = 36;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("cannot flush an empty memtable to segment")]
    EmptySegment,
    #[error("corrupted segment header or checksum mismatch")]
    CorruptedSegment,
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
    #[inline]
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
        dir: impl AsRef<Path>,
        segment_id: u64,
        table: &ColumnarMemTable,
    ) -> Result<SegmentZoneMap, StorageError> {
        let Some((min_ts, max_ts)) = table.min_max_ts() else {
            return Err(StorageError::EmptySegment);
        };
        let row_count = table.len();
        let path = dir.as_ref().join(format!("seg_{segment_id:08}.prs"));

        let payload_len = row_count * (8 + 4 + 8 + 8 + 4);
        let mut payload = Vec::with_capacity(payload_len);

        for &ts in &table.timestamps {
            payload.extend_from_slice(&ts.to_le_bytes());
        }
        for &sym in &table.symbols {
            payload.extend_from_slice(&sym.to_le_bytes());
        }
        for &val in &table.values {
            payload.extend_from_slice(&val.to_bits().to_le_bytes());
        }
        for &w in &table.weights {
            payload.extend_from_slice(&w.to_bits().to_le_bytes());
        }
        for &fl in &table.flags {
            payload.extend_from_slice(&fl.to_le_bytes());
        }

        let payload_crc32 = crc32fast::hash(&payload);

        let mut header = [0u8; PRS_HEADER_BYTES];
        header[0..4].copy_from_slice(&PRS_MAGIC);
        header[4..8].copy_from_slice(&1u32.to_le_bytes());
        header[8..16].copy_from_slice(&segment_id.to_le_bytes());
        header[16..24].copy_from_slice(&min_ts.to_le_bytes());
        header[24..32].copy_from_slice(&max_ts.to_le_bytes());
        header[32..36].copy_from_slice(&(row_count as u32).to_le_bytes());

        let mut file = File::create(&path)?;
        file.write_all(&header)?;
        file.write_all(&payload_crc32.to_le_bytes())?;
        file.write_all(&payload)?;
        file.sync_data()?;

        Ok(SegmentZoneMap {
            segment_id,
            path,
            min_ts,
            max_ts,
            row_count,
            payload_crc32,
        })
    }

    pub fn read_zone_map(path: impl AsRef<Path>) -> Result<SegmentZoneMap, StorageError> {
        let path_buf = path.as_ref().to_path_buf();
        let mut file = File::open(&path_buf)?;
        let mut buf = [0u8; PRS_HEADER_BYTES + 4];
        file.read_exact(&mut buf)
            .map_err(|_| StorageError::CorruptedSegment)?;

        if buf[0..4] != PRS_MAGIC {
            return Err(StorageError::CorruptedSegment);
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != 1 {
            return Err(StorageError::CorruptedSegment);
        }
        let segment_id = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let min_ts = i64::from_le_bytes(buf[16..24].try_into().unwrap());
        let max_ts = i64::from_le_bytes(buf[24..32].try_into().unwrap());
        let row_count = u32::from_le_bytes(buf[32..36].try_into().unwrap()) as usize;
        let payload_crc32 = u32::from_le_bytes(buf[36..40].try_into().unwrap());

        Ok(SegmentZoneMap {
            segment_id,
            path: path_buf,
            min_ts,
            max_ts,
            row_count,
            payload_crc32,
        })
    }

    pub fn read_segment(path: impl AsRef<Path>) -> Result<ColumnarMemTable, StorageError> {
        let mut file = File::open(path.as_ref())?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        if bytes.len() < PRS_HEADER_BYTES + 4 {
            return Err(StorageError::CorruptedSegment);
        }
        if bytes[0..4] != PRS_MAGIC {
            return Err(StorageError::CorruptedSegment);
        }
        let row_count = u32::from_le_bytes(bytes[32..36].try_into().unwrap()) as usize;
        let expected_crc = u32::from_le_bytes(bytes[36..40].try_into().unwrap());
        let payload = &bytes[40..];
        let expected_payload_len = row_count * (8 + 4 + 8 + 8 + 4);
        if payload.len() != expected_payload_len {
            return Err(StorageError::CorruptedSegment);
        }
        let actual_crc = crc32fast::hash(payload);
        if expected_crc != actual_crc {
            return Err(StorageError::CorruptedSegment);
        }

        let mut table = ColumnarMemTable::with_capacity(row_count);
        let mut cursor = 0usize;

        for _ in 0..row_count {
            let ts = i64::from_le_bytes(payload[cursor..cursor + 8].try_into().unwrap());
            table.timestamps.push(ts);
            cursor += 8;
        }
        for _ in 0..row_count {
            let sym = u32::from_le_bytes(payload[cursor..cursor + 4].try_into().unwrap());
            table.symbols.push(sym);
            cursor += 4;
        }
        for _ in 0..row_count {
            let val = f64::from_bits(u64::from_le_bytes(
                payload[cursor..cursor + 8].try_into().unwrap(),
            ));
            table.values.push(val);
            cursor += 8;
        }
        for _ in 0..row_count {
            let w = f64::from_bits(u64::from_le_bytes(
                payload[cursor..cursor + 8].try_into().unwrap(),
            ));
            table.weights.push(w);
            cursor += 8;
        }
        for _ in 0..row_count {
            let fl = u32::from_le_bytes(payload[cursor..cursor + 4].try_into().unwrap());
            table.flags.push(fl);
            cursor += 4;
        }
        table.rebuild_block_summaries();

        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prasasti_wal::WalRecord;
    use tempfile::tempdir;

    #[test]
    fn req_prs_05_segment_roundtrip_and_zone_map_overlap() {
        let dir = tempdir().unwrap();
        let mut table = ColumnarMemTable::with_capacity(2);
        table.push(WalRecord {
            timestamp_ns: 1_000,
            symbol_id: 0,
            value: 100.5,
            weight: 2.0,
            flags: 0,
        });
        table.push(WalRecord {
            timestamp_ns: 2_000,
            symbol_id: 0,
            value: 102.5,
            weight: 4.0,
            flags: 1,
        });

        let zm = SegmentStorage::write_segment(dir.path(), 1, &table).unwrap();
        assert_eq!(zm.min_ts, 1_000);
        assert_eq!(zm.max_ts, 2_000);
        assert_eq!(zm.row_count, 2);
        assert!(zm.overlaps(1_500, 3_000));
        assert!(!zm.overlaps(2_001, 3_000));

        let zm_disk = SegmentStorage::read_zone_map(&zm.path).unwrap();
        assert_eq!(zm, zm_disk);

        let loaded = SegmentStorage::read_segment(&zm.path).unwrap();
        assert_eq!(loaded, table);
    }
}
