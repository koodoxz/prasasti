#![forbid(unsafe_code)]

use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use clap::{Parser, Subcommand};
use prasasti_query::{AggregationSummary, PrasastiEngine, PrasastiError};
use prasasti_storage::{SegmentZoneMap, StorageTierHook};
use prasasti_wal::WalRecord;
use serde_json::{json, Value};
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
    /// Run as a native Model Context Protocol (MCP) JSON-RPC stdio server
    Mcp,
}

struct AuditHook {
    flushed: Mutex<Vec<SegmentZoneMap>>,
}

impl StorageTierHook for AuditHook {
    fn on_segment_flushed(&self, zone_map: &SegmentZoneMap) {
        self.flushed.lock().unwrap().push(zone_map.clone());
    }
}

fn validate_data_dir(data_dir: &str) -> Result<PathBuf, String> {
    if data_dir.trim().is_empty() {
        return Err("security error: data_dir must not be empty".to_string());
    }
    let path_obj = Path::new(data_dir);
    if path_obj
        .components()
        .any(|c| matches!(c, Component::ParentDir))
    {
        return Err(
            "security error: path traversal '..' is strictly forbidden in data_dir".to_string(),
        );
    }
    Ok(path_obj.to_path_buf())
}

fn run_self_audit() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut log =
        vec!["Running Prasasti 4-Stage Durability & Analytical Self-Audit...".to_string()];
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
    log.push(
        "  [PASS] Stage 1: CRC32 WAL Corrupted-Tail Recovery (15 bytes truncated cleanly)"
            .to_string(),
    );

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
    log.push(
        "  [PASS] Stage 2: Immutable .prs Segment Flush, Zone-Map Pruning & StorageTierHook"
            .to_string(),
    );

    // Stage 3: Native OHLCV & VWAP Exactness
    let candles = engine2.ohlcv_buckets("BTCUSD", 0, 20_000, 5_000)?;
    assert_eq!(candles.len(), 2);
    assert_eq!(candles[0].open, 60_000.0);
    assert_eq!(candles[0].close, 61_000.0);
    assert!((candles[0].vwap - 60_500.0).abs() < 1e-9);
    log.push("  [PASS] Stage 3: Native OHLCV Bucket Resampler & VWAP Exactness".to_string());

    // Stage 4: TimesFM Foundation Model Patch Slicer
    let patches = engine2.extract_timesfm_patches("BTCUSD", 0, 20_000, 2)?;
    assert_eq!(patches.len(), 2);
    assert_eq!(patches[0], vec![60_000.0, 61_000.0]);
    log.push("  [PASS] Stage 4: Google TimesFM / Kronos Contiguous Patch Extractor".to_string());

    log.push("ALL 4 PRASASTI DURABILITY & ANALYTICAL GATES PASSED.".to_string());
    Ok(log)
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
            for line in run_self_audit()? {
                println!("{line}");
            }
        }
        Commands::Inspect { dir } => {
            let engine = PrasastiEngine::open(&dir, 100_000)?;
            let summary = engine.aggregate_range(None, i64::MIN, i64::MAX)?;
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
        Commands::Mcp => {
            let stdin = io::stdin();
            let mut out = io::stdout();

            for line in stdin.lock().lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let req: Value = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let id = req.get("id").cloned().unwrap_or(Value::Null);
                let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");

                match method {
                    "initialize" => {
                        let resp = json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "protocolVersion": "2024-11-05",
                                "capabilities": {
                                    "tools": {}
                                },
                                "serverInfo": {
                                    "name": "prasasti-mcp-server",
                                    "version": env!("CARGO_PKG_VERSION")
                                }
                            }
                        });
                        writeln!(out, "{}", serde_json::to_string(&resp)?)?;
                        out.flush()?;
                    }
                    "notifications/initialized" => {}
                    "tools/list" => {
                        let resp = json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "tools": [
                                    {
                                        "name": "prasasti_record_telemetry",
                                        "description": "Record an AI agent step, tool invocation, or model API telemetry event into Prasasti's hardware-CRC32 Write-Ahead Log and columnar SoA table.",
                                        "inputSchema": {
                                            "type": "object",
                                            "properties": {
                                                "data_dir": {
                                                    "type": "string",
                                                    "description": "Path to the Prasasti telemetry storage directory"
                                                },
                                                "symbol": {
                                                    "type": "string",
                                                    "description": "Tool name, action signature, or model identifier (e.g. bash_exec, cargo_test, gemini_flash)"
                                                },
                                                "latency_ms": {
                                                    "type": "number",
                                                    "description": "Step execution latency in milliseconds"
                                                },
                                                "tokens": {
                                                    "type": "number",
                                                    "description": "Token quota consumed by this step (default: 1.0)"
                                                },
                                                "ts_ns": {
                                                    "type": "integer",
                                                    "description": "Optional timestamp in nanoseconds since Unix epoch"
                                                },
                                                "flags": {
                                                    "type": "integer",
                                                    "description": "Optional bitmask flags (0 = OK, 1 = ERROR)"
                                                }
                                            },
                                            "required": ["data_dir", "symbol", "latency_ms"]
                                        }
                                    },
                                    {
                                        "name": "prasasti_check_circuit",
                                        "description": "Evaluate microsecond sliding-window telemetry to detect runaway recursive tool loops (LOOP_DETECTED) or token budget exhaustion (QUOTA_EXCEEDED).",
                                        "inputSchema": {
                                            "type": "object",
                                            "properties": {
                                                "data_dir": {
                                                    "type": "string",
                                                    "description": "Path to the Prasasti telemetry storage directory"
                                                },
                                                "symbol": {
                                                    "type": "string",
                                                    "description": "Tool name, action signature, or model identifier to inspect"
                                                },
                                                "max_repeats": {
                                                    "type": "integer",
                                                    "description": "Maximum allowed invocations in the window before tripping LOOP_DETECTED (default: 5)"
                                                },
                                                "max_tokens": {
                                                    "type": "number",
                                                    "description": "Maximum allowed cumulative tokens in the window before tripping QUOTA_EXCEEDED (default: 50000.0)"
                                                },
                                                "start_ts": {
                                                    "type": "integer",
                                                    "description": "Window start timestamp in nanoseconds"
                                                },
                                                "end_ts": {
                                                    "type": "integer",
                                                    "description": "Window end timestamp in nanoseconds"
                                                }
                                            },
                                            "required": ["data_dir", "symbol"]
                                        }
                                    },
                                    {
                                        "name": "prasasti_audit_self",
                                        "description": "Execute Prasasti's 4-stage hardware CRC32 WAL crash recovery, Zone-Map pruning, OHLCV, and TimesFM patch self-audit.",
                                        "inputSchema": {
                                            "type": "object",
                                            "properties": {}
                                        }
                                    }
                                ]
                            }
                        });
                        writeln!(out, "{}", serde_json::to_string(&resp)?)?;
                        out.flush()?;
                    }
                    "tools/call" => {
                        let params = req.get("params").cloned().unwrap_or(Value::Null);
                        let tool_name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
                        let args = params.get("arguments").cloned().unwrap_or(Value::Null);

                        if tool_name == "prasasti_audit_self" {
                            match run_self_audit() {
                                Ok(lines) => {
                                    let resp = json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "content": [{ "type": "text", "text": lines.join("\n") }]
                                        }
                                    });
                                    writeln!(out, "{}", serde_json::to_string(&resp)?)?;
                                    out.flush()?;
                                }
                                Err(e) => {
                                    let resp = json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "isError": true,
                                            "content": [{ "type": "text", "text": format!("Prasasti audit error: {e}") }]
                                        }
                                    });
                                    writeln!(out, "{}", serde_json::to_string(&resp)?)?;
                                    out.flush()?;
                                }
                            }
                        } else if tool_name == "prasasti_record_telemetry" {
                            let data_dir =
                                args.get("data_dir").and_then(|d| d.as_str()).unwrap_or("");
                            let symbol = args
                                .get("symbol")
                                .and_then(|s| s.as_str())
                                .unwrap_or("unknown");
                            let latency_ms = args
                                .get("latency_ms")
                                .and_then(|v| v.as_f64())
                                .unwrap_or(0.0);
                            let tokens = args.get("tokens").and_then(|w| w.as_f64()).unwrap_or(1.0);
                            let ts_ns = args.get("ts_ns").and_then(|t| t.as_i64());
                            let flags =
                                args.get("flags").and_then(|f| f.as_u64()).unwrap_or(0) as u32;

                            let outcome: Result<Value, String> = (|| {
                                let dir_path = validate_data_dir(data_dir)?;
                                let mut engine = PrasastiEngine::open(&dir_path, 50_000)
                                    .map_err(|e| e.to_string())?;
                                let timestamp = ts_ns.unwrap_or_else(|| {
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_nanos() as i64
                                });
                                engine
                                    .append(symbol, timestamp, latency_ms, tokens, flags)
                                    .map_err(|e| e.to_string())?;
                                engine.sync().map_err(|e| e.to_string())?;
                                Ok(json!({
                                    "status": "RECORDED",
                                    "symbol": symbol,
                                    "ts_ns": timestamp,
                                    "latency_ms": latency_ms,
                                    "tokens": tokens,
                                    "flags": flags
                                }))
                            })();

                            match outcome {
                                Ok(val) => {
                                    let resp = json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "content": [{ "type": "text", "text": serde_json::to_string_pretty(&val)? }]
                                        }
                                    });
                                    writeln!(out, "{}", serde_json::to_string(&resp)?)?;
                                    out.flush()?;
                                }
                                Err(err_msg) => {
                                    let resp = json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "isError": true,
                                            "content": [{ "type": "text", "text": err_msg }]
                                        }
                                    });
                                    writeln!(out, "{}", serde_json::to_string(&resp)?)?;
                                    out.flush()?;
                                }
                            }
                        } else if tool_name == "prasasti_check_circuit" {
                            let data_dir =
                                args.get("data_dir").and_then(|d| d.as_str()).unwrap_or("");
                            let symbol = args
                                .get("symbol")
                                .and_then(|s| s.as_str())
                                .unwrap_or("unknown");
                            let max_repeats =
                                args.get("max_repeats")
                                    .and_then(|r| r.as_u64())
                                    .unwrap_or(5) as usize;
                            let max_tokens = args
                                .get("max_tokens")
                                .and_then(|t| t.as_f64())
                                .unwrap_or(50_000.0);
                            let start_ts = args
                                .get("start_ts")
                                .and_then(|t| t.as_i64())
                                .unwrap_or(i64::MIN);
                            let end_ts = args
                                .get("end_ts")
                                .and_then(|t| t.as_i64())
                                .unwrap_or(i64::MAX);

                            let outcome: Result<Value, String> = (|| {
                                let dir_path = validate_data_dir(data_dir)?;
                                let engine = PrasastiEngine::open(&dir_path, 50_000)
                                    .map_err(|e| e.to_string())?;
                                let summary =
                                    match engine.aggregate_range(Some(symbol), start_ts, end_ts) {
                                        Ok(s) => s,
                                        Err(PrasastiError::SymbolNotFound(_)) => {
                                            AggregationSummary {
                                                count: 0,
                                                min: 0.0,
                                                max: 0.0,
                                                sum: 0.0,
                                                mean: 0.0,
                                                vwap: 0.0,
                                                total_weight: 0.0,
                                                scanned_segments: 0,
                                                skipped_segments: 0,
                                            }
                                        }
                                        Err(e) => return Err(e.to_string()),
                                    };

                                let (status, reason) = if summary.count >= max_repeats {
                                    ("TRIP_CIRCUIT", "LOOP_DETECTED")
                                } else if summary.total_weight >= max_tokens {
                                    ("TRIP_CIRCUIT", "QUOTA_EXCEEDED")
                                } else {
                                    ("ALLOW", "OK")
                                };

                                Ok(json!({
                                    "status": status,
                                    "reason": reason,
                                    "symbol": symbol,
                                    "repeat_count": summary.count,
                                    "max_repeats": max_repeats,
                                    "total_tokens": summary.total_weight,
                                    "max_tokens": max_tokens,
                                    "mean_latency_ms": summary.mean,
                                    "max_latency_ms": summary.max,
                                    "scanned_segments": summary.scanned_segments,
                                    "skipped_segments": summary.skipped_segments
                                }))
                            })();

                            match outcome {
                                Ok(val) => {
                                    let resp = json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "content": [{ "type": "text", "text": serde_json::to_string_pretty(&val)? }]
                                        }
                                    });
                                    writeln!(out, "{}", serde_json::to_string(&resp)?)?;
                                    out.flush()?;
                                }
                                Err(err_msg) => {
                                    let resp = json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "result": {
                                            "isError": true,
                                            "content": [{ "type": "text", "text": err_msg }]
                                        }
                                    });
                                    writeln!(out, "{}", serde_json::to_string(&resp)?)?;
                                    out.flush()?;
                                }
                            }
                        } else {
                            let err_resp = json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": {
                                    "code": -32601,
                                    "message": format!("Unknown tool: {tool_name}")
                                }
                            });
                            writeln!(out, "{}", serde_json::to_string(&err_resp)?)?;
                            out.flush()?;
                        }
                    }
                    _ => {
                        let err_resp = json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": {
                                "code": -32601,
                                "message": format!("Method not found: {method}")
                            }
                        });
                        writeln!(out, "{}", serde_json::to_string(&err_resp)?)?;
                        out.flush()?;
                    }
                }
            }
        }
    }

    Ok(())
}
