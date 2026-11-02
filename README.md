# prasasti

Zero-allocation embedded columnar time-series storage and telemetry engine.

Part of the Nusantara Suite under the MECOB architecture.

## Overview

`prasasti` provides persistent, crash-resilient time-series ingestion and sub-millisecond range aggregation. It is engineered for high-frequency financial tick streaming, execution logging, and automated tensor extraction for time-series foundation models including Google TimesFM and Tsinghua Kronos.

### Core Guarantees

1. CRC32 Framed Write-Ahead Log: Sequential binary write log protected by hardware-accelerated IEEE CRC32 checksums. If ungraceful termination occurs mid-write, recovery automatically preserves all valid records and truncates corrupted trailing bytes without data loss.
2. Structure-of-Arrays (SoA) In-Memory Storage: Columnar memory layout optimized for cache locality and SIMD execution.
3. Symbol Interning: O(1) two-way string interning eliminating repeated string allocations during ingestion.
4. Block Synopsis Index: 1,024-row contiguous block summaries maintaining precomputed extrema, sums, and weights for rapid range folding.
5. Immutable Disk Segments with Zone-Map Pruning: Background flush to `.prs` files with timestamp bounds (`[min_ts, max_ts]`), allowing queries outside the time interval to bypass disk I/O entirely.
6. Native Tensor Patch Slicing: Direct extraction of contiguous float windows formatted for zero-copy ingestion by Google TimesFM 2.5 and Kronos models.
7. Zero Unsafe Code: The entire workspace strictly enforces `#![forbid(unsafe_code)]`.

## Workspace Structure

The workspace is organized into five discrete crates:

- `crates/prasasti-wal`: 36-byte binary framed Write-Ahead Log with CRC32 verification and crash recovery.
- `crates/prasasti-memtable`: Columnar Structure-of-Arrays buffer, block synopses, and symbol interner.
- `crates/prasasti-storage`: Immutable columnar `.prs` segment format, zone maps, and storage hooks.
- `crates/prasasti-query`: Range aggregation engine, OHLCV bucket resampler, and TimesFM patch extractor.
- `crates/prasasti-cli`: Command-line tool for ingestion, benchmarking, self-audit, and inspection.

## Verified Benchmarks

Environment: `x86_64-pc-windows-msvc`, Release Profile (`cargo build --release`), 1,000,000 events.

```
Single-Thread Write Throughput      : 18,502,757 events/sec
Batched Append Duration (WAL + SoA) : 54,046 us (54.05 ms)
1M-Row Vectorized Scan Latency p50  : 2 us (0.002 ms)
1M-Row Vectorized Scan Latency p99  : 18 us (0.018 ms)
Aggregated Metrics                  : Count, Min, Max, Sum, Mean, VWAP
Write SLA (>= 1,000,000 eps)        : PASSED
Query SLA (1M rows p50 <= 500 us)   : PASSED
```

## Building and Verification

### Prerequisites

- Rust 1.80+ (stable toolchain)
- Cargo

### Test Suite Execution

Run the workspace test suite:

```bash
cargo test --workspace --all-targets
```

Run static analysis with zero tolerance for warnings:

```bash
cargo clippy --all-targets -- -D warnings
```

Execute the built-in 4-stage durability and analytical self-audit:

```bash
cargo run --release -p prasasti-cli -- audit-self
```

Run the 1,000,000-row performance benchmark:

```bash
cargo run --release -p prasasti-cli -- bench --rows 1000000
```

## CLI Usage

### Ingesting an Event / Market Tick

```bash
cargo run --release -p prasasti-cli -- ingest --dir path/to/data --symbol XAUUSD --value 2350.25 --weight 10.0
```

### Extracting Tensor Patches for Foundation Models

```bash
cargo run --release -p prasasti-cli -- extract-patches --dir path/to/data --symbol XAUUSD --patch-len 32
```

### Range Aggregation Inspection

```bash
cargo run --release -p prasasti-cli -- inspect --dir path/to/data
```

## Storage Format Specification

Each `.prs` file consists of:

- 36-byte header: Magic bytes (`PRS1`), schema version, segment identifier, minimum timestamp, maximum timestamp, and row count.
- 4-byte payload CRC32 checksum.
- Contiguous column payloads: `timestamps: [i64]`, `symbols: [u32]`, `values: [f64]`, `weights: [f64]`, `flags: [u32]`.

## License

Dual-licensed under MIT OR Apache-2.0.
