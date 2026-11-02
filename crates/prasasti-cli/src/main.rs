#![forbid(unsafe_code)]

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use clap::{Parser, Subcommand};
use prasasti_query::PrasastiEngine;
use prasasti_storage::{SegmentZoneMap, StorageTierHook};
use prasasti_wal::WalRecord;
use tempfile::tempdir;

#[derive(Parser)]
#[command(
    name = "prasasti",
    version,
    about = "Zero-allocation embedded columnar time-series & telemetry engine"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Ingest a single event or tick into the engine
    Ingest {
        /// Path to the Prasasti data directory
        #[arg(long)]
        dir: PathBuf,
        /// Asset symbol (e.g. XAUUSD, BTCUSD)
        #[arg(long)]
        symbol: String,
        /// Unix timestamp in nanoseconds (optional, defaults to current UTC time)
        #[arg(long)]
        ts_ns: Option<i64>,
        /// Metric or price value
        #[arg(long)]
        value: f64,
        /// Weight or volume
        #[arg(long, default_value_t = 1.0)]
        weight: f64,
        /// Flags bitmask
        #[arg(long, default_value_t = 0)]
        flags: u32,
    },
    /// Extract contiguous float patches for Google TimesFM 2.5 / Kronos ingestion
    ExtractPatches {
        /// Path to the Prasasti data directory
        #[arg(long)]
        dir: PathBuf,
        /// Asset symbol
        #[arg(long)]
        symbol: String,
        /// Patch window size (default: 32)
        #[arg(long, default_value_t = 32)]
        patch_len: usize,
        /// Start timestamp in nanoseconds
        #[arg(long, default_value_t = 0)]
        start_ts: i64,
        /// End timestamp in nanoseconds
        #[arg(long, default_value_t = i64::MAX)]
        end_ts: i64,
    },
    /// Run 1,000,000-row write throughput and vectorized sub-millisecond aggregation benchmark
    Bench {
        /// Number of events to ingest and query
        #[arg(long, default_value_t = 1_000_000)]
        rows: usize,
    },
    /// Run 4-Stage Durability, Zone-Map Pruning, OHLCV, and TimesFM Slicer Self-Audit
    AuditSelf,
    /// Inspect a Prasasti data directory and run a full range summary
    Inspect {
        /// Path to the Prasasti data directory
        dir: PathBuf,
    },
}

struct AuditHook {
    flushed: Mutex<Vec<SegmentZoneMap>>,
}

impl StorageTierHook for AuditHook {
    fn on_segment_flushed(&self, zone_map: &SegmentZoneMap) {
        self.flushed.lock().unwrap().push(zone_map.clone());
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Ingest {
            dir,
            symbol,
            ts_ns,
            value,
            weight,
            flags,
        } => {
            let mut engine = PrasastiEngine::open(&dir, 50_000)?;
            let timestamp = ts_ns.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64
            });
            engine.append(&symbol, timestamp, value, weight, flags)?;
            engine.sync()?;
            println!(
                "{{\"status\":\"OK\",\"symbol\":\"{}\",\"ts_ns\":{},\"value\":{},\"weight\":{}}}",
                symbol, timestamp, value, weight
            );
        }
        Commands::ExtractPatches {
            dir,
            symbol,
            patch_len,
            start_ts,
            end_ts,
        } => {
            let engine = PrasastiEngine::open(&dir, 50_000)?;
            let patches = engine.extract_timesfm_patches(&symbol, start_ts, end_ts, patch_len)?;
            println!("{}", serde_json::to_string(&patches)?);
        }
        Commands::Bench { rows } => {
            let dir = tempdir()?;
            // Keep all 1,000,000 rows in the active SoA MemTable during the in-memory query SLA test
            let mut engine = PrasastiEngine::open(dir.path(), rows + 10_000)?;
            let sym_id = engine.intern_symbol("XAUUSD")?;

            let mut batch = Vec::with_capacity(rows);
            for i in 0..rows {
                batch.push(WalRecord {
                    timestamp_ns: 1_700_000_000_000_000_000 + (i as i64) * 1_000,
                    symbol_id: sym_id,
                    value: 2300.0 + ((i % 1000) as f64) * 0.05,
                    weight: 1.0 + ((i % 10) as f64) * 0.5,
                    flags: 0,
                });
            }

            let t_write = Instant::now();
            engine.append_batch(&batch)?;
            let write_us = t_write.elapsed().as_micros() as u64;
            let throughput_eps = (rows as f64) / (write_us as f64 / 1_000_000.0);

            let start_ts = 1_700_000_000_000_000_000;
            let end_ts = start_ts + (rows as i64) * 1_000;

            let mut query_latencies = Vec::with_capacity(50);
            let mut last_summary = None;
            for _ in 0..50 {
                let t_q = Instant::now();
                let summary = engine.aggregate_range(None, start_ts, end_ts)?;
                query_latencies.push(t_q.elapsed().as_micros() as u64);
                last_summary = Some(summary);
            }
            query_latencies.sort_unstable();
            let q_p50 = query_latencies[25];
            let q_p99 = query_latencies[49];
            let summary = last_summary.unwrap();

            println!("==================================================================");
            println!("PRASASTI COLUMNAR TIME-SERIES BENCHMARK ({} ROWS)", rows);
            println!("==================================================================");
            println!(
                "Batched Append Duration (WAL + SoA) : {} us ({:.2} ms)",
                write_us,
                write_us as f64 / 1000.0
            );
            println!(
                "Single-Thread Write Throughput      : {:.0} events/sec",
                throughput_eps
            );
            println!(
                "1M-Row Vectorized Scan Latency p50  : {} us ({:.3} ms)",
                q_p50,
                q_p50 as f64 / 1000.0
            );
            println!(
                "1M-Row Vectorized Scan Latency p99  : {} us ({:.3} ms)",
                q_p99,
                q_p99 as f64 / 1000.0
            );
            println!(
                "Aggregated Count / VWAP             : {} rows / {:.4}",
                summary.count, summary.vwap
            );
            println!(
                "Write SLA (>= 1,000,000 eps)        : {}",
                if throughput_eps >= 1_000_000.0 {
                    "PASSED"
                } else {
                    "FAILED"
                }
            );
            println!(
                "Query SLA (1M rows p50 <= 500 us)   : {}",
                if q_p50 <= 500 { "PASSED" } else { "FAILED" }
            );
            println!("==================================================================");
        }
        Commands::AuditSelf => {
            println!("Running Prasasti 4-Stage Durability & Analytical Self-Audit...");
            let dir = tempdir()?;

            // Stage 1: WAL Corrupted Tail Crash Recovery
            {
                let mut engine = PrasastiEngine::open(dir.path(), 100)?;
                engine.append("XAUUSD", 1_000, 2350.0, 2.0, 0)?;
                engine.append("XAUUSD", 2_000, 2355.0, 2.0, 0)?;
                engine.sync()?;
            }
            {
                let wal_path = dir.path().join("active.wal");
                let mut f = OpenOptions::new().append(true).open(&wal_path)?;
                f.write_all(&[0xBA, 0xAD, 0xF0, 0x0D, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])?;
                f.sync_all()?;
            }
            let reopened = PrasastiEngine::open(dir.path(), 100)?;
            assert_eq!(reopened.recovered_truncated_bytes(), 15);
            assert_eq!(reopened.memtable_len(), 2);
            println!("  [PASS] Stage 1: CRC32 WAL Corrupted-Tail Recovery (15 bytes truncated cleanly)");

            // Stage 2: Immutable Segment Flush & Min-Max Zone-Map Pruning
            let dir2 = tempdir()?;
            let hook = Arc::new(AuditHook {
                flushed: Mutex::new(Vec::new()),
            });
            let mut engine2 = PrasastiEngine::open(dir2.path(), 2)?.with_storage_tier_hook(hook.clone());
            engine2.append("BTCUSD", 1_000, 60_000.0, 1.0, 0)?;
            engine2.append("BTCUSD", 2_000, 61_000.0, 1.0, 0)?;
            engine2.append("BTCUSD", 10_000, 62_000.0, 2.0, 0)?;
            engine2.append("BTCUSD", 11_000, 63_000.0, 2.0, 0)?;
            let sum = engine2.aggregate_range(Some("BTCUSD"), 10_000, 11_000)?;
            assert_eq!(sum.count, 2);
            assert_eq!(sum.scanned_segments, 1);
            assert_eq!(sum.skipped_segments, 1);
            assert_eq!(hook.flushed.lock().unwrap().len(), 2);
            println!("  [PASS] Stage 2: Immutable .prs Segment Flush, Zone-Map Pruning & StorageTierHook");

            // Stage 3: Native OHLCV & VWAP Exactness
            let candles = engine2.ohlcv_buckets("BTCUSD", 0, 20_000, 5_000)?;
            assert_eq!(candles.len(), 2);
            assert_eq!(candles[0].open, 60_000.0);
            assert_eq!(candles[0].close, 61_000.0);
            assert!((candles[0].vwap - 60_500.0).abs() < 1e-9);
            println!("  [PASS] Stage 3: Native OHLCV Bucket Resampler & VWAP Exactness");

            // Stage 4: TimesFM Foundation Model Patch Slicer
            let patches = engine2.extract_timesfm_patches("BTCUSD", 0, 20_000, 2)?;
            assert_eq!(patches.len(), 2);
            assert_eq!(patches[0], vec![60_000.0, 61_000.0]);
            println!("  [PASS] Stage 4: Google TimesFM / Kronos Contiguous Patch Extractor");

            println!("ALL 4 PRASASTI DURABILITY & ANALYTICAL GATES PASSED.");
        }
        Commands::Inspect { dir } => {
            let engine = PrasastiEngine::open(&dir, 100_000)?;
            let summary = engine.aggregate_range(None, i64::MIN, i64::MAX)?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
    }

    Ok(())
}
