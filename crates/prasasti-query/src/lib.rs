#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use prasasti_memtable::{ColumnarMemTable, SymbolInterner, BLOCK_SIZE};
use prasasti_storage::{SegmentStorage, SegmentZoneMap, StorageError, StorageTierHook};
use prasasti_wal::{WalError, WalRecord, WriteAheadLog};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PrasastiError {
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
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

type OhlcvAccum = (f64, f64, f64, f64, f64, f64, usize);

pub struct PrasastiEngine {
    data_dir: PathBuf,
    max_memtable_rows: usize,
    wal: WriteAheadLog,
    memtable: ColumnarMemTable,
    interner: SymbolInterner,
    segments: Vec<SegmentZoneMap>,
    next_segment_id: u64,
    recovered_truncated_bytes: u64,
    tier_hook: Option<Arc<dyn StorageTierHook>>,
}

impl PrasastiEngine {
    pub fn open(data_dir: impl AsRef<Path>, max_memtable_rows: usize) -> Result<Self, PrasastiError> {
        if max_memtable_rows == 0 {
            return Err(PrasastiError::InvalidConfig(
                "max_memtable_rows must be > 0".to_string(),
            ));
        }
        let dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let symbols_path = dir.join("symbols.json");
        let interner: SymbolInterner = if symbols_path.exists() {
            let bytes = fs::read(&symbols_path)?;
            serde_json::from_slice(&bytes).unwrap_or_default()
        } else {
            SymbolInterner::new()
        };

        let mut segments = Vec::new();
        let mut max_seg_id = 0u64;
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "prs") {
                let zm = SegmentStorage::read_zone_map(&path)?;
                if zm.segment_id > max_seg_id {
                    max_seg_id = zm.segment_id;
                }
                segments.push(zm);
            }
        }
        segments.sort_by_key(|z| z.segment_id);

        let wal_path = dir.join("active.wal");
        let recovery = WriteAheadLog::recover(&wal_path)?;
        let mut memtable = ColumnarMemTable::with_capacity(max_memtable_rows.min(65_536));
        for rec in recovery.records {
            memtable.push(rec);
        }

        let wal = WriteAheadLog::open(&wal_path)?;

        Ok(Self {
            data_dir: dir,
            max_memtable_rows,
            wal,
            memtable,
            interner,
            segments,
            next_segment_id: max_seg_id + 1,
            recovered_truncated_bytes: recovery.truncated_tail_bytes,
            tier_hook: None,
        })
    }

    pub fn with_storage_tier_hook(mut self, hook: Arc<dyn StorageTierHook>) -> Self {
        self.tier_hook = Some(hook);
        self
    }

    pub fn recovered_truncated_bytes(&self) -> u64 {
        self.recovered_truncated_bytes
    }

    pub fn memtable_len(&self) -> usize {
        self.memtable.len()
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn intern_symbol(&mut self, symbol: &str) -> Result<u32, PrasastiError> {
        let prev_len = self.interner.len();
        let id = self.interner.intern(symbol);
        if self.interner.len() != prev_len {
            self.persist_symbols()?;
        }
        Ok(id)
    }

    fn persist_symbols(&self) -> Result<(), PrasastiError> {
        let symbols_path = self.data_dir.join("symbols.json");
        let json = serde_json::to_vec(&self.interner)
            .map_err(|e| PrasastiError::InvalidConfig(e.to_string()))?;
        fs::write(symbols_path, json)?;
        Ok(())
    }

    #[inline]
    pub fn append(
        &mut self,
        symbol: &str,
        timestamp_ns: i64,
        value: f64,
        weight: f64,
        flags: u32,
    ) -> Result<(), PrasastiError> {
        let symbol_id = self.intern_symbol(symbol)?;
        self.append_by_id(symbol_id, timestamp_ns, value, weight, flags)
    }

    #[inline]
    pub fn append_by_id(
        &mut self,
        symbol_id: u32,
        timestamp_ns: i64,
        value: f64,
        weight: f64,
        flags: u32,
    ) -> Result<(), PrasastiError> {
        let rec = WalRecord {
            timestamp_ns,
            symbol_id,
            value,
            weight,
            flags,
        };
        self.wal.append(&rec)?;
        self.memtable.push(rec);

        if self.memtable.len() >= self.max_memtable_rows {
            let _ = self.flush_memtable()?;
        }
        Ok(())
    }

    pub fn append_batch(&mut self, records: &[WalRecord]) -> Result<(), PrasastiError> {
        self.wal.append_batch(records)?;
        for &rec in records {
            self.memtable.push(rec);
            if self.memtable.len() >= self.max_memtable_rows {
                let _ = self.flush_memtable()?;
            }
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), PrasastiError> {
        self.wal.sync()?;
        self.persist_symbols()?;
        Ok(())
    }

    pub fn flush_memtable(&mut self) -> Result<Option<SegmentZoneMap>, PrasastiError> {
        if self.memtable.is_empty() {
            return Ok(None);
        }
        let seg_id = self.next_segment_id;
        self.next_segment_id += 1;

        let zone_map = SegmentStorage::write_segment(&self.data_dir, seg_id, &self.memtable)?;
        self.memtable.clear();
        self.wal.clear()?;
        self.persist_symbols()?;

        if let Some(hook) = &self.tier_hook {
            hook.on_segment_flushed(&zone_map);
        }
        self.segments.push(zone_map.clone());
        Ok(Some(zone_map))
    }

    pub fn aggregate_range(
        &self,
        symbol: Option<&str>,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<AggregationSummary, PrasastiError> {
        let target_sym_id = match symbol {
            Some(s) => Some(
                self.interner
                    .get_id(s)
                    .ok_or_else(|| PrasastiError::SymbolNotFound(s.to_string()))?,
            ),
            None => None,
        };

        let mut count = 0usize;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        let mut sum = 0.0f64;
        let mut weighted_sum = 0.0f64;
        let mut total_weight = 0.0f64;
        let mut scanned_segments = 0usize;
        let mut skipped_segments = 0usize;

        for zm in &self.segments {
            if !zm.overlaps(start_ts, end_ts) {
                skipped_segments += 1;
                continue;
            }
            scanned_segments += 1;
            let seg_table = SegmentStorage::read_segment(&zm.path)?;
            Self::accumulate_table(
                &seg_table,
                target_sym_id,
                start_ts,
                end_ts,
                &mut count,
                &mut min,
                &mut max,
                &mut sum,
                &mut weighted_sum,
                &mut total_weight,
            );
        }

        Self::accumulate_table(
            &self.memtable,
            target_sym_id,
            start_ts,
            end_ts,
            &mut count,
            &mut min,
            &mut max,
            &mut sum,
            &mut weighted_sum,
            &mut total_weight,
        );

        if count == 0 {
            return Ok(AggregationSummary {
                count: 0,
                min: 0.0,
                max: 0.0,
                sum: 0.0,
                mean: 0.0,
                vwap: 0.0,
                total_weight: 0.0,
                scanned_segments,
                skipped_segments,
            });
        }

        let mean = sum / (count as f64);
        let vwap = if total_weight > 0.0 {
            weighted_sum / total_weight
        } else {
            mean
        };

        Ok(AggregationSummary {
            count,
            min,
            max,
            sum,
            mean,
            vwap,
            total_weight,
            scanned_segments,
            skipped_segments,
        })
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn accumulate_table(
        table: &ColumnarMemTable,
        target_sym_id: Option<u32>,
        start_ts: i64,
        end_ts: i64,
        count: &mut usize,
        min: &mut f64,
        max: &mut f64,
        sum: &mut f64,
        weighted_sum: &mut f64,
        total_weight: &mut f64,
    ) {
        let (lo, hi) = table.time_range_bounds(start_ts, end_ts);
        if lo >= hi {
            return;
        }

        let mut cur = lo;
        while cur < hi {
            let block_idx = cur / BLOCK_SIZE;
            if cur.is_multiple_of(BLOCK_SIZE)
                && cur + BLOCK_SIZE <= hi
                && block_idx < table.block_summaries.len()
            {
                let blk = &table.block_summaries[block_idx];
                match target_sym_id {
                    None => {
                        *count += BLOCK_SIZE;
                        if blk.min < *min {
                            *min = blk.min;
                        }
                        if blk.max > *max {
                            *max = blk.max;
                        }
                        *sum += blk.sum;
                        *weighted_sum += blk.weighted_sum;
                        *total_weight += blk.total_weight;
                        cur += BLOCK_SIZE;
                        continue;
                    }
                    Some(sym_id) => {
                        if let Some(blk_sym) = blk.single_symbol {
                            if blk_sym == sym_id {
                                *count += BLOCK_SIZE;
                                if blk.min < *min {
                                    *min = blk.min;
                                }
                                if blk.max > *max {
                                    *max = blk.max;
                                }
                                *sum += blk.sum;
                                *weighted_sum += blk.weighted_sum;
                                *total_weight += blk.total_weight;
                            }
                            cur += BLOCK_SIZE;
                            continue;
                        }
                    }
                }
            }

            let step_end = ((block_idx + 1) * BLOCK_SIZE).min(hi);
            for i in cur..step_end {
                if target_sym_id.is_none_or(|sym_id| table.symbols[i] == sym_id) {
                    let v = table.values[i];
                    let w = table.weights[i];
                    *count += 1;
                    if v < *min {
                        *min = v;
                    }
                    if v > *max {
                        *max = v;
                    }
                    *sum += v;
                    *weighted_sum += v * w;
                    *total_weight += w;
                }
            }
            cur = step_end;
        }
    }

    pub fn ohlcv_buckets(
        &self,
        symbol: &str,
        start_ts: i64,
        end_ts: i64,
        interval_ns: i64,
    ) -> Result<Vec<OhlcvBucket>, PrasastiError> {
        if interval_ns <= 0 {
            return Err(PrasastiError::InvalidConfig(
                "interval_ns must be > 0".to_string(),
            ));
        }
        let sym_id = self
            .interner
            .get_id(symbol)
            .ok_or_else(|| PrasastiError::SymbolNotFound(symbol.to_string()))?;

        // BTreeMap keyed by bucket_start_ns -> (open, high, low, close, volume, weighted_sum, count)
        let mut map: BTreeMap<i64, OhlcvAccum> = BTreeMap::new();

        for zm in &self.segments {
            if !zm.overlaps(start_ts, end_ts) {
                continue;
            }
            let seg = SegmentStorage::read_segment(&zm.path)?;
            Self::collect_ohlcv(&seg, sym_id, start_ts, end_ts, interval_ns, &mut map);
        }
        Self::collect_ohlcv(
            &self.memtable,
            sym_id,
            start_ts,
            end_ts,
            interval_ns,
            &mut map,
        );

        let mut out = Vec::with_capacity(map.len());
        for (bucket_start_ns, (open, high, low, close, volume, weighted_sum, count)) in map {
            let vwap = if volume > 0.0 {
                weighted_sum / volume
            } else {
                close
            };
            out.push(OhlcvBucket {
                bucket_start_ns,
                open,
                high,
                low,
                close,
                volume,
                vwap,
                count,
            });
        }
        Ok(out)
    }

    fn collect_ohlcv(
        table: &ColumnarMemTable,
        sym_id: u32,
        start_ts: i64,
        end_ts: i64,
        interval_ns: i64,
        map: &mut BTreeMap<i64, OhlcvAccum>,
    ) {
        let (lo, hi) = table.time_range_bounds(start_ts, end_ts);
        for i in lo..hi {
            if table.symbols[i] != sym_id {
                continue;
            }
            let ts = table.timestamps[i];
            let val = table.values[i];
            let w = table.weights[i];
            let bucket_start = (ts / interval_ns) * interval_ns;

            map.entry(bucket_start)
                .and_modify(|e| {
                    if val > e.1 {
                        e.1 = val;
                    }
                    if val < e.2 {
                        e.2 = val;
                    }
                    e.3 = val;
                    e.4 += w;
                    e.5 += val * w;
                    e.6 += 1;
                })
                .or_insert((val, val, val, val, w, val * w, 1));
        }
    }

    /// Extracts fixed-size contiguous float patches (`patch_len`, e.g. 32 or 128) for direct feeding
    /// into Google TimesFM 2.5 or Kronos foundation models (`REQ-PRS-04`).
    pub fn extract_timesfm_patches(
        &self,
        symbol: &str,
        start_ts: i64,
        end_ts: i64,
        patch_len: usize,
    ) -> Result<Vec<Vec<f64>>, PrasastiError> {
        if patch_len == 0 {
            return Err(PrasastiError::InvalidConfig(
                "patch_len must be > 0".to_string(),
            ));
        }
        let sym_id = self
            .interner
            .get_id(symbol)
            .ok_or_else(|| PrasastiError::SymbolNotFound(symbol.to_string()))?;

        let mut series = Vec::new();
        for zm in &self.segments {
            if !zm.overlaps(start_ts, end_ts) {
                continue;
            }
            let seg = SegmentStorage::read_segment(&zm.path)?;
            let (lo, hi) = seg.time_range_bounds(start_ts, end_ts);
            for i in lo..hi {
                if seg.symbols[i] == sym_id {
                    series.push(seg.values[i]);
                }
            }
        }
        let (lo, hi) = self.memtable.time_range_bounds(start_ts, end_ts);
        for i in lo..hi {
            if self.memtable.symbols[i] == sym_id {
                series.push(self.memtable.values[i]);
            }
        }

        let patches: Vec<Vec<f64>> = series
            .chunks_exact(patch_len)
            .map(|chunk| chunk.to_vec())
            .collect();
        Ok(patches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::tempdir;

    struct MockTierHook {
        flushed: Mutex<Vec<SegmentZoneMap>>,
    }

    impl StorageTierHook for MockTierHook {
        fn on_segment_flushed(&self, zone_map: &SegmentZoneMap) {
            self.flushed.lock().unwrap().push(zone_map.clone());
        }
    }

    #[test]
    fn req_prs_03_04_05_06_end_to_end_engine_aggregation_ohlcv_and_pruning() {
        let dir = tempdir().unwrap();
        let hook = Arc::new(MockTierHook {
            flushed: Mutex::new(Vec::new()),
        });

        // max_memtable_rows = 2 -> every 2 rows auto-flushes a .prs segment on disk
        let mut engine = PrasastiEngine::open(dir.path(), 2)
            .expect("engine must open")
            .with_storage_tier_hook(hook.clone());

        // Segment 1: timestamps [1_000, 2_000]
        engine.append("XAUUSD", 1_000, 2000.0, 1.0, 0).unwrap();
        engine.append("XAUUSD", 2_000, 2010.0, 3.0, 0).unwrap();
        assert_eq!(engine.segment_count(), 1);

        // Segment 2: timestamps [10_000, 11_000]
        engine.append("XAUUSD", 10_000, 2050.0, 2.0, 0).unwrap();
        engine.append("XAUUSD", 11_000, 2060.0, 2.0, 0).unwrap();
        assert_eq!(engine.segment_count(), 2);

        // Active MemTable: timestamp [20_000]
        engine.append("XAUUSD", 20_000, 2100.0, 2.0, 0).unwrap();
        engine.sync().unwrap();

        // Verify StorageTierHook recorded 2 flushed segments
        assert_eq!(hook.flushed.lock().unwrap().len(), 2);

        // Query only [1_000, 2_000] -> must scan Segment 1 and skip Segment 2!
        let summary = engine
            .aggregate_range(Some("XAUUSD"), 1_000, 2_000)
            .unwrap();
        assert_eq!(summary.count, 2);
        assert_eq!(summary.min, 2000.0);
        assert_eq!(summary.max, 2010.0);
        assert!((summary.vwap - 2007.5).abs() < 1e-9);
        assert_eq!(summary.scanned_segments, 1);
        assert_eq!(summary.skipped_segments, 1);

        // Query OHLCV across [0, 25_000] with interval 10_000
        let candles = engine.ohlcv_buckets("XAUUSD", 0, 25_000, 10_000).unwrap();
        assert_eq!(candles.len(), 3);
        assert_eq!(candles[0].bucket_start_ns, 0);
        assert_eq!(candles[0].open, 2000.0);
        assert_eq!(candles[0].close, 2010.0);
        assert_eq!(candles[0].volume, 4.0);

        // Verify TimesFM patch slicer (patch_len = 2 -> 2 full patches from 5 points)
        let patches = engine
            .extract_timesfm_patches("XAUUSD", 0, 25_000, 2)
            .unwrap();
        assert_eq!(patches, vec![vec![2000.0, 2010.0], vec![2050.0, 2060.0]]);
    }
}
