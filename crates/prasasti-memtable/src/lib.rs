#![forbid(unsafe_code)]

use std::collections::HashMap;
use prasasti_wal::WalRecord;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SymbolInterner {
    to_id: HashMap<String, u32>,
    to_str: Vec<String>,
}

impl SymbolInterner {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn intern(&mut self, symbol: &str) -> u32 {
        if let Some(&id) = self.to_id.get(symbol) {
            return id;
        }
        let id = self.to_str.len() as u32;
        let owned = symbol.to_string();
        self.to_str.push(owned.clone());
        self.to_id.insert(owned, id);
        id
    }

    #[inline]
    pub fn resolve(&self, id: u32) -> Option<&str> {
        self.to_str.get(id as usize).map(String::as_str)
    }

    #[inline]
    pub fn get_id(&self, symbol: &str) -> Option<u32> {
        self.to_id.get(symbol).copied()
    }

    pub fn len(&self) -> usize {
        self.to_str.len()
    }

    pub fn is_empty(&self) -> bool {
        self.to_str.is_empty()
    }
}

pub const BLOCK_SIZE: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BlockSummary {
    pub single_symbol: Option<u32>,
    pub min: f64,
    pub max: f64,
    pub sum: f64,
    pub weighted_sum: f64,
    pub total_weight: f64,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnarMemTable {
    pub timestamps: Vec<i64>,
    pub symbols: Vec<u32>,
    pub values: Vec<f64>,
    pub weights: Vec<f64>,
    pub flags: Vec<u32>,
    pub block_summaries: Vec<BlockSummary>,
}

impl ColumnarMemTable {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            timestamps: Vec::with_capacity(cap),
            symbols: Vec::with_capacity(cap),
            values: Vec::with_capacity(cap),
            weights: Vec::with_capacity(cap),
            flags: Vec::with_capacity(cap),
            block_summaries: Vec::with_capacity(cap / BLOCK_SIZE + 1),
        }
    }

    #[inline]
    pub fn push(&mut self, rec: WalRecord) {
        if let Some(&last_ts) = self.timestamps.last() {
            if rec.timestamp_ns < last_ts {
                // Out-of-order insertion path: binary search insertion index and invalidate block summaries
                let idx = self.timestamps.partition_point(|&ts| ts <= rec.timestamp_ns);
                self.timestamps.insert(idx, rec.timestamp_ns);
                self.symbols.insert(idx, rec.symbol_id);
                self.values.insert(idx, rec.value);
                self.weights.insert(idx, rec.weight);
                self.flags.insert(idx, rec.flags);
                self.rebuild_block_summaries();
                return;
            }
        }
        self.timestamps.push(rec.timestamp_ns);
        self.symbols.push(rec.symbol_id);
        self.values.push(rec.value);
        self.weights.push(rec.weight);
        self.flags.push(rec.flags);

        if self.timestamps.len().is_multiple_of(BLOCK_SIZE) {
            let start = self.timestamps.len() - BLOCK_SIZE;
            let summary = self.compute_block_summary(start, start + BLOCK_SIZE);
            self.block_summaries.push(summary);
        }
    }

    pub fn rebuild_block_summaries(&mut self) {
        self.block_summaries.clear();
        let full_blocks = self.timestamps.len() / BLOCK_SIZE;
        self.block_summaries.reserve(full_blocks);
        for b in 0..full_blocks {
            let start = b * BLOCK_SIZE;
            let summary = self.compute_block_summary(start, start + BLOCK_SIZE);
            self.block_summaries.push(summary);
        }
    }

    #[inline]
    fn compute_block_summary(&self, start: usize, end: usize) -> BlockSummary {
        let first_sym = self.symbols[start];
        let mut same_sym = true;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        let mut sum = 0.0f64;
        let mut weighted_sum = 0.0f64;
        let mut total_weight = 0.0f64;

        for i in start..end {
            if self.symbols[i] != first_sym {
                same_sym = false;
            }
            let v = self.values[i];
            let w = self.weights[i];
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
            sum += v;
            weighted_sum += v * w;
            total_weight += w;
        }

        BlockSummary {
            single_symbol: if same_sym { Some(first_sym) } else { None },
            min,
            max,
            sum,
            weighted_sum,
            total_weight,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    pub fn clear(&mut self) {
        self.timestamps.clear();
        self.symbols.clear();
        self.values.clear();
        self.weights.clear();
        self.flags.clear();
        self.block_summaries.clear();
    }

    /// Returns `(start_idx, end_idx)` slice bounds covering `[start_ts, end_ts]` inclusive in $O(\log N)$ time.
    #[inline]
    pub fn time_range_bounds(&self, start_ts: i64, end_ts: i64) -> (usize, usize) {
        if self.timestamps.is_empty() || start_ts > end_ts {
            return (0, 0);
        }
        let lo = self.timestamps.partition_point(|&ts| ts < start_ts);
        let hi = self.timestamps.partition_point(|&ts| ts <= end_ts);
        (lo, hi)
    }

    pub fn min_max_ts(&self) -> Option<(i64, i64)> {
        if self.timestamps.is_empty() {
            None
        } else {
            Some((self.timestamps[0], *self.timestamps.last().unwrap()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn req_prs_02_symbol_interner_and_soa_memtable_ordering() {
        let mut interner = SymbolInterner::new();
        let id_xau = interner.intern("XAUUSD");
        let id_btc = interner.intern("BTCUSD");
        assert_eq!(id_xau, interner.intern("XAUUSD"));
        assert_ne!(id_xau, id_btc);
        assert_eq!(interner.resolve(id_xau), Some("XAUUSD"));
        assert_eq!(interner.get_id("BTCUSD"), Some(id_btc));
        assert_eq!(interner.len(), 2);

        let mut table = ColumnarMemTable::with_capacity(4);
        table.push(WalRecord {
            timestamp_ns: 200,
            symbol_id: id_xau,
            value: 2010.0,
            weight: 2.0,
            flags: 0,
        });
        // Out-of-order insert at ts = 100
        table.push(WalRecord {
            timestamp_ns: 100,
            symbol_id: id_xau,
            value: 2000.0,
            weight: 1.0,
            flags: 0,
        });
        table.push(WalRecord {
            timestamp_ns: 300,
            symbol_id: id_xau,
            value: 2020.0,
            weight: 3.0,
            flags: 0,
        });

        assert_eq!(table.len(), 3);
        assert_eq!(table.timestamps, vec![100, 200, 300]);
        assert_eq!(table.time_range_bounds(150, 250), (1, 2));
        assert_eq!(table.min_max_ts(), Some((100, 300)));
    }
}
