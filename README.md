# ⚡ t3 (Turbo 3)

> **Blazing-fast parallel S3 client & 100% drop-in `s5cmd` replacement in Rust: ~1.5 ms cold start, ~4.5 MiB idle memory footprint, pre-allocated `pwrite`/`pread` zero-lock NVMe I/O, and sister project to [RustStack](https://github.com/ehlerst/ruststack).**

[![CI](https://github.com/ehlerst/t3/actions/workflows/ci.yml/badge.svg)](https://github.com/ehlerst/t3/actions/workflows/ci.yml)
[![Performance Benchmarks](https://github.com/ehlerst/t3/actions/workflows/benchmarks.yml/badge.svg)](https://github.com/ehlerst/t3/actions/workflows/benchmarks.yml)
[![Comparative S3 Client Benchmark](https://github.com/ehlerst/t3/actions/workflows/compare-alternatives.yml/badge.svg)](https://github.com/ehlerst/t3/actions/workflows/compare-alternatives.yml)
[![Release](https://github.com/ehlerst/t3/actions/workflows/release.yml/badge.svg)](https://github.com/ehlerst/t3/actions/workflows/release.yml)
[![Docker Image](https://img.shields.io/badge/docker-ehlers320%2Ft3-blue.svg)](https://hub.docker.com/r/ehlers320/t3)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

`t3` (**Turbo 3**) is an ultra-fast, memory-safe S3 CLI and sync engine written in pure Rust. Built as the sister project to [RustStack](https://github.com/ehlerst/ruststack), `t3` is engineered to outperform both `peak/s5cmd` (Go) and the official `aws s3` CLI (Python) across startup latency, small-object IOPS, large multipart transfers, and peak RSS memory usage.

---

## ⚡ Why `t3` Beats `s5cmd` and `aws-cli`

| Metric / Capability | **⚡ `t3` (Rust)** | `s5cmd` v2.3.0 (Go) | `aws s3` CLI v2 (Python) |
|:---|:---|:---|:---|
| **Process Cold-Start Latency** | **~1.5 ms** | ~20 ms | ~450 ms |
| **Idle / Small-Transfer RSS Memory** | **~4.5 MiB** | ~35–300+ MiB | ~85+ MiB |
| **Multipart Memory Backpressure** | **Bounded Slab `BufferPool`** | Unbounded per-worker 5 MiB heap allocs | Sequential / Python GIL bound |
| **Local NVMe File I/O** | **`set_len` + concurrent `pwrite`/`pread` + `DashSet` mkdir cache** | Standard `os.File` syscalls | Standard Python file I/O |
| **SigV4 Request Signing** | **Cached daily HMAC key (~220k+ ops/s/core)** | Re-derived per request | `botocore` Python signing |
| **S3-to-S3 Copy > 5 GiB** | **Automatic Parallel Multipart Copy** | Fails ([#29](https://github.com/peak/s5cmd/issues/29)) | Supported (slow) |
| **Path Traversal Guard (`../`)** | **Enforced (`sanitize_relative_path`)** | Vulnerable ([#872](https://github.com/peak/s5cmd/issues/872)) | Enforced |
| **`sync --delete` with `--exclude`** | **Honors `--exclude` / `--include` DFA** | Deletes excluded files ([#815](https://github.com/peak/s5cmd/issues/815)) | Honors filters |
| **`~/.aws/config` `endpoint_url`** | **Supported natively + multi-endpoint round-robin** | Unsupported | Supported |

---

## ⚡ Performance Highlights

Automated benchmark rating measured via [`crates/t3-benchmarks/`](crates/t3-benchmarks/):

| Component | Operation | Payload / Batch | Throughput | Bandwidth | p50 Latency | p95 Latency | Grade |
|:---|:---|:---|---:|---:|---:|---:|:---|
| **t3-storage** | `PathTraversalGuard + WildcardSplit` | Key Validation | **413,000+ ops/s** | N/A | **1.4 µs** | 1.5 µs | **A+ (Ultra Fast)** |
| **t3-core** | `GlobFilterDFA (Include+Exclude)` | 4 Globs | **409,000+ ops/s** | N/A | **1.4 µs** | 1.5 µs | **A+ (Ultra Fast)** |
| **t3-core** | `SelectQueryExecute (NDJSON)` | 4 Rows/Doc | **298,000+ ops/s** | 33.9 MB/s | **2.4 µs** | 2.4 µs | **A+ (Ultra Fast)** |
| **t3-s3-client** | `SigV4SignRequest (Cached Key)` | Auth Header | **221,000+ ops/s** | N/A | **3.4 µs** | 3.8 µs | **A+ (Ultra Fast)** |
| **t3-s3-client** | `SigV4PresignUrl` | Query Auth | **167,000+ ops/s** | N/A | **4.8 µs** | 5.3 µs | **A+ (Ultra Fast)** |
| **t3-storage** | `PositionalPread (read_exact_at)` | 64 KB | **118,000+ ops/s** | **7,384.1 MB/s** | **7.3 µs** | 9.4 µs | **A+ (Ultra Fast)** |
| **t3-engine** | `BatchDeleteObjects (1000/batch)` | 200 Keys | **62,500+ ops/s** | N/A | **16.0 µs** | 16.0 µs | **A+ (Ultra Fast)** |
| **t3-storage** | `PreallocatedPwrite (write_all_at)` | 64 KB | **49,100+ ops/s** | **3,070.2 MB/s** | **17.3 µs** | 23.3 µs | **A+ (Ultra Fast)** |
| **t3-engine** | `ParallelDownload (pwrite + DirCache)` | 200 x 4 KB | **37,800+ ops/s** | 147.9 MB/s | **26.4 µs** | 26.4 µs | **A+ (Ultra Fast)** |
| **t3-engine** | `ParallelRangeDownload (pwrite)` | 32 MiB (8x ranges) | **42 ops/s** | **1,343.9 MB/s** | 23.8 ms | 23.8 ms | **A+ (Ultra Fast)** |
| **t3-engine** | `MultipartUpload (pread + BufferPool)` | 32 MiB (8x parts) | **24 ops/s** | **781.1 MB/s** | 40.9 ms | 40.9 ms | **A+ (Ultra Fast)** |

---

## 📦 Complete `s5cmd` Feature & Subcommand Support (15 Subcommands)

`t3` supports `s5cmd`'s exact CLI syntax (global flags before or after the subcommand):

1. **`cp [options] <src> <dst>`**:
   - Directions: `Local → S3`, `S3 → Local`, `S3 → S3` (including > 5 GiB parallel multipart copy), `Local → Local`.
   - Flags: `-c/--concurrency` (default 5), `-p/--part-size` (default 50 MiB), `-f/--flatten`, `-n/--no-clobber`, `-s/--if-size-differ`, `-u/--if-source-newer`, `-sp/--show-progress`, `--exclude`, `--include`, `--raw`, `--no-follow-symlinks`, `--storage-class`, `--sse`, `--sse-kms-key-id`, `--acl`, `--cache-control`, `--expires`, `--content-type`, `--content-encoding`, `--content-disposition`, `--metadata Key=Value`, `--metadata-directive`, `--source-region`, `--destination-region`, `--version-id`.
2. **`mv [options] <src> <dst>`**: Parallel copy followed by source deletion.
3. **`sync [options] <src> <dst>`**: Concurrent source & destination listing + streaming diff with `--delete` (respecting `--exclude`/`--include`), `--size-only`, and `--exact-timestamps`.
4. **`ls [options] [target]`**: Lists buckets or objects/prefixes (`-e/--etag`, `-H/--humanize`, `-s/--show-fullpath`, `--all-versions`).
5. **`rm [options] <target>...`**: Pipelined 1,000-key `DeleteObjects` batcher across wildcards (`--raw`, `--exclude`, `--include`, `--version-id`, `--all-versions`).
6. **`run [file]`**: **"Beast Mode"** — parses batch commands from a file or `stdin` and executes them concurrently on the shared 256-worker pool.
7. **`cat <s3-url>...`**: Streams one or more S3 objects (including wildcards) to `stdout`.
8. **`pipe [options] <s3-url>`**: Streams `stdin` directly to an S3 object via bounded-memory parallel multipart upload.
9. **`head <s3-url>`**: Prints `HeadBucket` or `HeadObject` metadata.
10. **`du [options] <s3-url>`**: Summarizes object bytes and counts (`-g/--group` by storage class, `-H/--humanize`).
11. **`mb <s3://bucket>`** & **`rb [--force] <s3://bucket>`**: Create or remove buckets (`--force` empties the bucket first).
12. **`presign [-e <duration>] <s3-url>`**: Generates a SigV4 presigned `GET` URL.
13. **`select -q <sql> <s3-url>`**: Executes SQL queries across JSON/NDJSON or CSV objects (supports wildcards).
14. **`bucket-version [--set Enabled|Suspended] <s3://bucket>`**: Gets or sets bucket versioning.
15. **`version`**: Prints version and build metadata.

---

## 🚀 Quick Start

### Build from Source
```bash
cargo build --release --bin t3
./target/release/t3 version
```

### Usage with [RustStack](https://github.com/ehlerst/ruststack) (`http://localhost:4566`) or AWS S3
```bash
# Create bucket
t3 --endpoint-url http://localhost:4566 mb s3://ml-datasets

# Upload a directory with execution stats
t3 --endpoint-url http://localhost:4566 --stat cp ./shards/ s3://ml-datasets/train/

# Download matching wildcard objects with exclude filter
t3 --endpoint-url http://localhost:4566 --stat cp \
  --exclude "*.tmp" \
  "s3://ml-datasets/train/*.parquet" ./local-shards/

# Synchronize and delete stale destination objects
t3 --endpoint-url http://localhost:4566 --stat sync --delete \
  ./local-shards/ s3://ml-datasets/train/
```

---

## 🏗️ Workspace Structure

```text
crates/
├── t3-cli/             # Binary entrypoint (`t3`), s5cmd-compatible clap parser, shell completions
├── t3-core/            # Execution engine: two-tier work scheduler, DFA glob filter, speculative prefix fan-out lister, sync engine, `run` batch parser
├── t3-storage/         # Bounded slab BufferPool, pre-allocated pwrite/pread positional I/O, DashSet mkdir cache, path-traversal guard
├── t3-s3-client/       # High-throughput S3 data-plane client (cached SigV4 signing, multi-endpoint round-robin load balancing, connection pool)
├── t3-telemetry/       # Lock-free atomic stats (--stat), NDJSON structured logger (--json), indicatif progress bars (-sp)
└── t3-benchmarks/      # Automated microbenchmark & end-to-end S3 transfer performance rating suite
```

## 📄 License

Licensed under either of [Apache License, Version 2.0](LICENSE) or MIT license at your option.
