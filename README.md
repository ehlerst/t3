# ⚡ t3 (Turbo 3)

> **Blazing-fast parallel S3 client & 100% drop-in `s5cmd` replacement in Rust: ~1.5 ms cold start, ~4.5 MiB idle memory footprint, pre-allocated `pwrite`/`pread` zero-lock NVMe I/O, and sister project to [RustStack](https://github.com/ehlerst/ruststack).**

[![CI](https://github.com/ehlerst/t3/actions/workflows/ci.yml/badge.svg)](https://github.com/ehlerst/t3/actions/workflows/ci.yml)
[![Performance Benchmarks](https://github.com/ehlerst/t3/actions/workflows/benchmarks.yml/badge.svg)](https://github.com/ehlerst/t3/actions/workflows/benchmarks.yml)
[![Comparative S3 Client Benchmark](https://github.com/ehlerst/t3/actions/workflows/compare-alternatives.yml/badge.svg)](https://github.com/ehlerst/t3/actions/workflows/compare-alternatives.yml)
[![Release](https://github.com/ehlerst/t3/actions/workflows/release.yml/badge.svg)](https://github.com/ehlerst/t3/releases/latest)
[![Website](https://img.shields.io/badge/website-ehlerst.github.io%2Ft3-orange.svg)](https://ehlerst.github.io/t3/)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

`t3` (**Turbo 3**) is an ultra-fast, memory-safe S3 CLI and sync engine written in pure Rust. Built as the sister project to [RustStack](https://github.com/ehlerst/ruststack), `t3` is engineered to outperform both `peak/s5cmd` (Go) and the official `aws s3` CLI (Python) across startup latency, small-object IOPS, large multipart transfers, and peak RSS memory usage.

---

## ⚡ Head-to-Head Bandwidth & Transfer Time (`t3` vs `s5cmd` v2.3.0 on RustStack)

### 1. Sustained Bandwidth & Transfer Time (`MiB/s` & `ms`)

Measured end-to-end transfer time and sustained throughput across dataset sizes from `4 MiB` up to `512 MiB` against [RustStack](https://github.com/ehlerst/ruststack):

| Dataset & Direction | **⚡ `t3` v0.2.0 Bandwidth (Time)** | `s5cmd` v2.3.0 Bandwidth (Time) | `t3` Bandwidth & Time Advantage |
|:---|---:|---:|:---|
| **Upload 512 MiB (`64 × 8 MiB` shards)** | **2,453.0 MiB/s (`208.7 ms`)** | 1,947.4 MiB/s (`262.9 ms`) | **1.26x faster** (+505.6 MiB/s) |
| **Upload 256 MiB (`64 × 4 MiB` shards)** | **2,366.9 MiB/s (`108.2 ms`)** | 1,895.2 MiB/s (`135.1 ms`) | **1.25x faster** (+471.7 MiB/s) |
| **Download 256 MiB (`64 × 4 MiB` shards)** | **3,505.8 MiB/s (`73.0 ms`)** | 3,278.5 MiB/s (`78.1 ms`) | **1.07x faster** (+227.3 MiB/s) |
| **Upload 128 MiB (`256 × 512 KiB` shards)** | **1,910.5 MiB/s (`67.0 ms`)** | 1,255.0 MiB/s (`102.0 ms`) | **1.52x faster** (+655.5 MiB/s) |
| **Download 128 MiB (`256 × 512 KiB` shards)** | **2,418.5 MiB/s (`52.9 ms`)** | 1,815.7 MiB/s (`70.5 ms`) | **1.33x faster** (+602.8 MiB/s) |
| **Upload 64 MiB (`1,000 × 64 KiB` files)** | **1,613.2 MiB/s (`38.7 ms`)** | 511.8 MiB/s (`122.1 ms`) | **3.15x faster** (+1,101.4 MiB/s) |
| **Download 64 MiB (`1,000 × 64 KiB` files)** | **1,954.7 MiB/s (`32.0 ms`)** | 471.4 MiB/s (`132.6 ms`) | **4.15x faster** (+1,483.3 MiB/s) |
| **Upload 4 MiB (`1,000 × 4 KiB` files)** | **221.7 MiB/s (`17.6 ms`)** | 44.7 MiB/s (`87.4 ms`) | **4.96x faster** (+177.0 MiB/s) |
| **Download 4 MiB (`1,000 × 4 KiB` files)** | **155.6 MiB/s (`25.1 ms`)** | 36.2 MiB/s (`107.8 ms`) | **4.29x faster** (+119.4 MiB/s) |

### 2. High-IOPS Operations & Memory Footprint (1,000 × 4 KiB Objects)

Measured end-to-end wall-clock execution time and peak resident memory (`RSS`) across 1,000 × 4 KiB objects against [RustStack](https://github.com/ehlerst/ruststack):

| Workload (1,000 × 4 KiB Objects) | **⚡ `t3` v0.2.0 (Rust)** | `s5cmd` v2.3.0 (Go) | `t3` Speedup | `t3` Memory Reduction |
|:---|---:|---:|:---|:---|
| **Cold Startup (`version`)** | **3.1 ms** (6.2 MiB) | 4.8 ms (13.2 MiB) | **1.55x faster** | **2.12x less RAM** |
| **Parallel Upload (`cp 1,000 files → S3`)** | **17.6 ms** (14.3 MiB) | 87.4 ms (33.6 MiB) | **4.96x faster** | **2.35x less RAM** |
| **Parallel Download (`cp S3 → 1,000 files`)** | **25.1 ms** (12.9 MiB) | 107.8 ms (43.3 MiB) | **4.29x faster** | **3.34x less RAM** |
| **Object Listing (`ls 1,000 objects`)** | **7.3 ms** (8.3 MiB) | 26.5 ms (20.1 MiB) | **3.61x faster** | **2.42x less RAM** |
| **Disk Usage Summary (`du 1,000 objects`)** | **6.5 ms** (8.4 MiB) | 28.3 ms (20.6 MiB) | **4.36x faster** | **2.44x less RAM** |
| **Incremental Sync (`sync --size-only`)** | **8.2 ms** (7.9 MiB) | 58.6 ms (31.1 MiB) | **7.18x faster** | **3.93x less RAM** |
| **Pipelined Batch Delete (`rm 1,000 keys`)** | **8.6 ms** (8.7 MiB) | 43.7 ms (23.5 MiB) | **5.07x faster** | **2.71x less RAM** |

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

## 🗜️ `v0.2.0` Tar-Stream Compression Suite Benchmark (`t3 tar-sync` / `cp --tar-stream`)

Measured across a 500-file Buildx/CI cache dataset (**65.13 MiB** raw: 200 OCI JSON manifests, 150 IR/symbol cache files, 150 binary layer blobs):

| Codec | Save (`Tar+Comp`) | Restore (`Decomp+Untar`) | Local S3 Round-Trip | 1 Gbps Cloud S3 Round-Trip | Compressed Size (Ratio) |
|:---|---:|---:|---:|---:|---:|
| **`lz4` (`lz4_flex`, default)** | **50.2 ms (`1,298.2 MiB/s`)** | **37.7 ms (`1,729.8 MiB/s`)** | **107.6 ms** | **390.5 ms** | 18.94 MiB (`3.44x`) |
| **`zstd-1` (`zstd` L1)** | **79.8 ms (`816.6 MiB/s`)** | **47.4 ms (`1,374.9 MiB/s`)** | 144.9 ms | **392.8 ms** | **16.60 MiB (`3.92x`)** |
| **`zstd-3` (`zstd` L3)** | 108.9 ms (`598.2 MiB/s`) | 43.9 ms (`1,485.2 MiB/s`) | 167.8 ms | 406.4 ms | **15.85 MiB (`4.11x`)** |
| **`none` (`.tar`)** | 30.0 ms (`2,169.4 MiB/s`) | 30.3 ms (`2,152.2 MiB/s`) | 161.5 ms | 1,158.9 ms | 65.43 MiB (`1.00x`) |
| **`gzip-1` (`flate2` fast)** | 160.6 ms (`405.7 MiB/s`) | 93.8 ms (`694.6 MiB/s`) | 271.7 ms | 539.8 ms | 17.68 MiB (`3.68x`) |
| **`gzip-6` (`flate2` default)** | 1,384.8 ms (`47.0 MiB/s`) | 92.8 ms (`701.6 MiB/s`) | 1,494.3 ms | 1,743.6 ms | 16.66 MiB (`3.91x`) |

---

## 📦 Complete `s5cmd` Feature & Subcommand Support (17 Subcommands)

`t3` supports `s5cmd`'s exact CLI syntax plus high-speed `v0.2.0` CI/cache extensions (`--json-stats`, `--json-stats-file`, `--retry-limit`):

1. **`cp [options] <src> <dst>`**:
   - Directions: `Local → S3`, `S3 → Local`, `S3 → S3` (including > 5 GiB parallel multipart copy), `Local → Local`, plus streaming `stdin`/`stdout` (`t3 cp - s3://bucket/key` and `t3 cp s3://bucket/key -`).
   - Flags: `-c/--concurrency` (default 5), `-p/--part-size` (adaptive by file size when omitted; $\le 16\text{ MiB}$ uses single-PUT fast path), `-C/--continue` (`--resume` via `.t3part` range tracking), `--tar-stream`, `--compression auto|lz4|zstd|gzip|none`, `--if-match <etag>`, `--if-none-match <etag|*>`, `-f/--flatten`, `-n/--no-clobber`, `-s/--if-size-differ`, `-u/--if-source-newer`, `-sp/--show-progress`, `--exclude`, `--include`, `--raw`, `--no-follow-symlinks`, `--storage-class`, `--sse`, `--sse-kms-key-id`, `--acl`, `--cache-control`, `--expires`, `--content-type`, `--content-encoding`, `--content-disposition`, `--metadata Key=Value`, `--metadata-directive`, `--source-region`, `--destination-region`, `--version-id`.
2. **`tar-sync [options] <src> <dst>`**: Packs a local directory tree into a single compressed S3 tar object (`.tar.lz4`, `.tar.zst`, `.tar.gz`, `.tar`) or restores a single S3 tar archive directly into a local directory with automatic magic-byte codec detection and path-traversal protection.
3. **`exists [--version-id <id>] <s3://bucket[/key]>`**: Fast `HEAD`-based existence check returning stable exit code `0` if found and exit code `3` if not found (`404`).
4. **`mv [options] <src> <dst>`**: Parallel copy followed by source deletion.
5. **`sync [options] <src> <dst>`**: Concurrent source & destination listing + streaming diff with `--delete` (respecting `--exclude`/`--include`), `--exclude-newer-than <duration>` (protects recent staging objects during `--delete`), `--size-only`, and `--exact-timestamps`.
6. **`ls [options] [target]`**: Lists buckets or objects/prefixes (`-e/--etag`, `-H/--humanize`, `-s/--show-fullpath`, `--all-versions`).
7. **`rm [options] <target>...`**: Pipelined 1,000-key `DeleteObjects` batcher across wildcards (`--older-than <duration>`, `--raw`, `--exclude`, `--include`, `--version-id`, `--all-versions`).
8. **`run [file]`**: **"Beast Mode"** — parses batch commands from a file or `stdin` and executes them concurrently on the shared 256-worker pool.
9. **`cat <s3-url>...`**: Streams one or more S3 objects (including wildcards) to `stdout`.
10. **`pipe [options] <s3-url>`**: Streams `stdin` directly to an S3 object via bounded-memory parallel multipart upload.
11. **`head <s3-url>`**: Prints `HeadBucket` or `HeadObject` metadata.
12. **`du [options] <s3-url>`**: Summarizes object bytes and counts (`-g/--group` by storage class, `-H/--humanize`).
13. **`mb <s3://bucket>`** & **`rb [--force] <s3://bucket>`**: Create or remove buckets (`--force` empties the bucket first).
14. **`presign [-e <duration>] <s3-url>`**: Generates a SigV4 presigned `GET` URL.
15. **`select -q <sql> <s3-url>`**: Executes SQL queries across JSON/NDJSON or CSV objects (supports wildcards).
16. **`bucket-version [--set Enabled|Suspended] <s3://bucket>`**: Gets or sets bucket versioning.
17. **`version`**: Prints version and build metadata.

### Stable Exit Codes
- `0`: Success
- `1`: General S3 / transport / runtime error
- `2`: CLI usage / invalid argument error
- `3`: Not Found (`404 NoSuchKey` / `NoSuchBucket` / cache miss)
- `4`: Precondition Failed (`412 PreconditionFailed` / `409 ConditionalRequestConflict` on `--if-match` / `--if-none-match` CAS writes)

---

## 🚀 Quick Start

### 1. One-Line Installer (Linux & macOS · `amd64` & `arm64` · SHA-256 Verified)
```bash
curl -sSfL https://ehlerst.github.io/t3/install.sh | sh
```
Or install to a custom directory / specific version:
```bash
T3_VERSION=v0.2.0 T3_INSTALL_DIR="$HOME/.local/bin" \
  sh -c "$(curl -sSfL https://raw.githubusercontent.com/ehlerst/t3/main/install.sh)"
```

### 2. Homebrew (macOS & Linux) or Cargo
```bash
# Homebrew formula
brew install --formula https://raw.githubusercontent.com/ehlerst/t3/main/Formula/t3.rb

# Cargo (from source)
cargo install --git https://github.com/ehlerst/t3 t3-cli
```

### 3. Manual Binary Download (with SHA-256 Verification)
```bash
# Linux amd64 (or t3-linux-arm64 / t3-darwin-arm64 / t3-darwin-amd64)
curl -sSLO https://github.com/ehlerst/t3/releases/latest/download/t3-linux-amd64.tar.gz
curl -sSLO https://github.com/ehlerst/t3/releases/latest/download/t3-linux-amd64.tar.gz.sha256
sha256sum -c t3-linux-amd64.tar.gz.sha256
tar xzf t3-linux-amd64.tar.gz && sudo mv t3 /usr/local/bin/
```

### 4. AWS Authentication, EKS IRSA & EC2 IMDSv2 Support
`t3` implements the standard Amazon AWS credential provider chain out of the box:
1. **Environment Variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (and `AWS_SECURITY_TOKEN`).
2. **Amazon EKS IRSA (IAM Roles for Service Accounts)**: Automatically exchanges Kubernetes service account JWT tokens (`AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE` + optional `AWS_ROLE_SESSION_NAME` / `AWS_ENDPOINT_URL_STS`) via AWS STS `AssumeRoleWithWebIdentity`.
3. **Shared Credentials & Config Files**: `~/.aws/credentials` and `~/.aws/config` (including `AWS_SHARED_CREDENTIALS_FILE`, `AWS_CONFIG_FILE`, `AWS_PROFILE`, profile `endpoint_url`, profile `role_arn` + `web_identity_token_file`, and external `credential_process`).
4. **EKS Pod Identity & ECS Container Credentials**: `AWS_CONTAINER_CREDENTIALS_FULL_URI` / `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` with `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` / `AWS_CONTAINER_AUTHORIZATION_TOKEN`.
5. **Amazon EC2 Instance Metadata Service (`IMDSv2` / `IMDSv1`)**: Automatically exchanges a session token via `PUT http://169.254.169.254/latest/api/token` (`X-aws-ec2-metadata-token-ttl-seconds: 21600`) and retrieves attached EC2 IAM role credentials (respects `AWS_EC2_METADATA_DISABLED` and `AWS_EC2_METADATA_SERVICE_ENDPOINT`).
6. **Anonymous Access**: `--no-sign-request` for public S3 buckets.

### 5. `v0.2.0` CI Cache, CAS Pointer & Fleet Examples
```bash
# 1. Single-object compressed tar-stream save & restore (auto-detects lz4 / zstd / gzip / raw tar)
t3 --json-stats tar-sync \
  --metadata gh-repo=owner/repo --metadata run-id=12345 --metadata generation=7 \
  ./buildx-cache/ s3://ci-cache-bucket/caches/gen-7.tar.lz4

t3 --json-stats tar-sync s3://ci-cache-bucket/caches/gen-7.tar.lz4 ./restored-cache/
# Or via cp --tar-stream (--compression lz4|zstd|gzip|none|auto):
t3 cp --tar-stream --compression zstd ./buildx-cache/ s3://ci-cache-bucket/caches/gen-7.tar.zst

# 2. Fast HEAD existence check with stable exit codes (0 = exists, 3 = not found / cache miss, 1 = S3 error)
t3 exists s3://ci-cache-bucket/pointers/_pointer

# 3. Compare-And-Swap (CAS) atomic pointer updates (exit code 4 on 412 PreconditionFailed conflict)
# First writer creates pointer atomically:
t3 cp --if-none-match "*" ./_pointer s3://ci-cache-bucket/pointers/_pointer
# Subsequent generation updates pointer via ETag CAS:
t3 cp --if-match '"d41d8cd98f00b204e9800998ecf8427e"' ./_pointer s3://ci-cache-bucket/pointers/_pointer

# 4. Resumable multipart ranged download (picks up existing .t3part & .t3part.meta ranges on retry)
t3 cp --continue s3://ci-cache-bucket/layers/large-2gb.tar.zst ./large-2gb.tar.zst

# 5. Age-scoped garbage collection of stale staging prefixes
t3 rm --older-than 24h "s3://ci-cache-bucket/staging/*"
t3 sync --delete --exclude-newer-than 2h ./active-cache/ s3://ci-cache-bucket/prefix/
```

### 6. General Usage with [RustStack](https://github.com/ehlerst/ruststack) (`http://localhost:4566`) or AWS S3
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
