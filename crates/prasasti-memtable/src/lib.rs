#![forbid(unsafe_code)]

use std::collections::HashMap;
use prasasti_wal::WalRecord;

#[derive(Debug, Default, Clone)]
pub struct SymbolInterner {
    to_id: HashMap<String, u32>,
    to_str: Vec<String>,
}

impl SymbolInterner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn intern(&mut self, _symbol: &str) -> u32 {
        0 // RED stub
    }

    pub fn resolve(&self, _id: u32) -> Option<&str> {
        None
    }

    pub fn get_id(&self, _symbol: &str) -> Option<u32> {
        None
    }
}

#[derive(Debug, Default, Clone)]
pub struct ColumnarMemTable {
    pub timestamps: Vec<i64>,
    pub symbols: Vec<u32>,
    pub values: Vec<f64>,
    pub weights: Vec<f64>,
    pub flags: Vec<u32>,
}

impl ColumnarMemTable {
    pub fn with_capacity(_cap: usize) -> Self {
        Self::default()
    }

    pub fn push(&mut self, _rec: WalRecord) {
        // RED stub
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    pub fn clear(&mut self) {
        self.timestamps.clear();
        self.symbols.clear();
        self.values.clear();
        self.weights.clear();
        self.flags.clear();
    }

    pub fn time_range_bounds(&self, _start_ts: i64, _end_ts: i64) -> (usize, usize) {
        (0, 0)
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

        let mut table = ColumnarMemTable::with_capacity(4);
        table.push(WalRecord {
            timestamp_ns: 100,
            symbol_id: id_xau,
            value: 2000.0,
            weight: 1.0,
            flags: 0,
        });
        table.push(WalRecord {
            timestamp_ns: 200,
            symbol_id: id_xau,
            value: 2010.0,
            weight: 2.0,
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
        assert_eq!(table.time_range_bounds(150, 250), (1, 2));
    }
}
