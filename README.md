# High-Performance Concurrent Log Aggregator & WAL Pipeline

A production-grade, highly concurrent log ingestion and aggregation engine built in Rust.
This system acts as a systems-level middleware layer capable of processing streaming text/JSON logs
over network connections, batching them using time-and-size thresholds, and writing them down to
a persistent, atomic Write-Ahead Log (WAL).

## 🏛️ Architectural Overview

```
[Log Producers] ─────(TCP Socket)──────> [ Async Network Ingestion ]
                                                     │
                                            (Lock-Free Channel)
                                                     ▼
                                           [ Batching Processor ]
                                           (Regex / Zero-copy Parsing)
                                                     │
                                           (Bounded mpsc Channel)
                                                     ▼
                                       ┌───────────────────────────┐
                                       │    Pluggable Storage      │
                                       ├─────────────┬─────────────┤
                                       │ Memory-Map  │  In-Memory  │
                                       │ (WAL/Disk)  │ (Ring Buff) │
                                       └─────────────┴─────────────┘
```

## 🚀 Getting Started

### 1. Run the Main Aggregator Server

To fire up the primary ingestion socket listener, run:

```bash
RUST_LOG=debug cargo run
```

### 2. Run the Stress Test Tool

To simulate intensive real-world network traffic against the active ingestion pipeline, open
a separate terminal window and execute:

```bash
cargo run --bin stress_test
```

