# prasasti

Zero-allocation embedded columnar time-series engine and Google TimesFM tensor slicer in Rust.

Part of the Nusantara Suite under the MECOB architecture.

## What is prasasti?

Traditional row-based databases (like SQLite) bottleneck when receiving millions of incoming ticks per second. Analytical engines (like DuckDB) perform well on large batch queries, but struggle with streaming single-event writes.

`prasasti` bridges this gap as an embedded columnar time-series storage engine written in Rust. It delivers 18+ million events/second write throughput with sequential Write-Ahead Logging (WAL) and executes range queries across 1,000,000 rows in 2 microseconds.

It also provides native tensor slicing, formatting raw tick data directly into contiguous float arrays for Google TimesFM 2.5 and Tsinghua Kronos forecasting models.

### Core Features

- Crash-Resilient Write-Ahead Log: Uses a 36-byte binary frame with hardware-accelerated CRC32 checksums. If a crash occurs mid-write, corrupted trailing bytes are automatically trimmed during recovery with zero loss of prior data.
- Columnar Structure-of-Arrays (SoA): Stores timestamps, symbols, prices, volumes, and flags in contiguous memory arrays for fast CPU cache access.
- Zero-Allocation Symbol Interning: Maps symbol strings (e.g., `XAUUSD`, `EURUSD`) to 32-bit IDs with O(1) lookups, eliminating repeated string heap allocations.
- Block Synopses: Maintains 1,024-row precalculated summaries (min, max, sum, volume-weighted sum) for rapid range evaluations.
- Immutable Segments with Zone-Map Pruning: Flushes full memory tables to `.prs` disk files with `[min_ts, max_ts]` bounds, allowing queries outside the time range to skip disk reads entirely.
- Direct TimesFM / Kronos Patch Slicing: Slices historical time-series data into fixed-size patch tensors without passing through Pandas or NumPy.
- Zero Unsafe Code: Built with `#![forbid(unsafe_code)]`.

## Workspace Crates

- `crates/prasasti-wal`: Binary framed Write-Ahead Log with CRC32 recovery.
- `crates/prasasti-memtable`: Columnar memory table, block synopses, and symbol interner.
- `crates/prasasti-storage`: Immutable `.prs` columnar disk segments and zone maps.
- `crates/prasasti-query`: Range query engine, OHLCV candle resampler, and patch slicer.
- `crates/prasasti-cli`: CLI binary for ingestion, benchmarks, self-audits, and inspection.

## Benchmark Results

Environment: `x86_64-pc-windows-msvc`, Release Profile, 1,000,000 events.

```
Single-Thread Write Throughput      : 18,502,757 events/sec
Batched Append Duration (WAL + SoA) : 54,046 us (54.05 ms)
1M-Row Vectorized Scan Latency p50  : 2 us (0.002 ms)
1M-Row Vectorized Scan Latency p99  : 18 us (0.018 ms)
Aggregated Metrics                  : Count, Min, Max, Sum, Mean, VWAP
Write SLA (>= 1,000,000 eps)        : PASSED
Query SLA (1M rows p50 <= 500 us)   : PASSED
```

## Quick Start

### Build and Test

```bash
cargo test --workspace --all-targets
cargo clippy --all-targets -- -D warnings
```

### Run Durability Self-Audit

Tests WAL power-loss recovery, zone-map pruning, and TimesFM patch slicing:

```bash
cargo run --release -p prasasti-cli -- audit-self
```

### Ingest an Event / Market Tick

```bash
cargo run --release -p prasasti-cli -- ingest --dir ./data --symbol XAUUSD --value 2350.25 --weight 10.0
```

### Extract Patches for TimesFM

```bash
cargo run --release -p prasasti-cli -- extract-patches --dir ./data --symbol XAUUSD --patch-len 32
```

## License

Dual-licensed under MIT OR Apache-2.0.
