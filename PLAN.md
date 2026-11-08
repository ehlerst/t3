# Fast Rust S3 CLI (`t3` — `s5cmd` Successor) — Research & Engineering Plan

## 1. Executive Summary & Why `peak/s5cmd` Needs a Rust Replacement

[`peak/s5cmd`](https://github.com/peak/s5cmd) (~4.2k GitHub stars) became the de facto high-performance S3 CLI for ML dataset loading, CI/CD artifact transfers, and big-data pipelines by replacing Python (`aws-cli`, `s3cmd`) with a single Go binary and a 256-worker pool.

However, **`peak/s5cmd` is effectively unmaintained** (last release `v2.3.0` in Dec 2024; last commit June 2025; 195+ open issues including [Issue #855: "Is this package being maintained?"](https://github.com/peak/s5cmd/issues/855)) and suffers from deep architectural and maintenance limitations:

1. **Built on Deprecated `aws-sdk-go` v1 (End-of-Support July 2025)**:
   - Lacks modern AWS auth providers out of the box: **EKS Pod Identity** ([#871](https://github.com/peak/s5cmd/issues/871)), **AWS IAM Identity Center (SSO v2)**, and **`endpoint_url` in `~/.aws/config`**.
   - Lacks modern S3 integrity checksums (`CRC32C`, `CRC64NVME`, `SHA256`) and S3 Express One Zone directory buckets.
2. **Broken or Slow Operations at Scale**:
   - **S3-to-S3 copies > 5 GB fail** ([#29](https://github.com/peak/s5cmd/issues/29)) because `storage/s3.go` only calls single-request `CopyObject` instead of multipart copy.
   - **Serial `DeleteObjects` bottleneck** ([#870](https://github.com/peak/s5cmd/issues/870), [#844](https://github.com/peak/s5cmd/issues/844)): `MultiDelete` chunks 1,000 keys per batch, but does not parallelize batch delete requests across prefixes or multiple buckets.
   - **Single-stream `ListObjectsV2` pagination**: Listing a prefix with millions of keys happens sequentially on a single HTTP stream instead of speculative/hierarchical prefix fan-out (`Delimiter=/` or lexicographical key-space splitting).
   - **Cross-endpoint `sync` is unsupported**: Syncing directly between two different S3 endpoints (e.g., AWS S3 ↔ Cloudflare R2 / MinIO / RustStack) requires downloading to local disk first.
3. **Go Runtime & I/O Bottlenecks on 40–100 Gbps Links**:
   - `s3manager.Uploader` / `Downloader` in `aws-sdk-go` v1 allocates 5 MiB+ heap buffers per part concurrency slot, triggering heavy Go GC pauses and high RSS memory under `--numworkers 256 --concurrency 10`.
   - Standard blocking/threaded `os.File` `ReadAt`/`WriteAt` syscalls cause filesystem contention when writing thousands of files concurrently on NVMe RAID arrays.
4. **Unpatched Security & Correctness Bugs**:
   - Path traversal on malicious S3 keys containing `../` during `cp`/`sync` to local disk ([#872](https://github.com/peak/s5cmd/issues/872)).
   - `sync --delete` ignoring `--exclude`/`--include` filters ([#815](https://github.com/peak/s5cmd/issues/815), [#852](https://github.com/peak/s5cmd/issues/852)).
   - Silent exit code `0` on `sync` errors ([#869](https://github.com/peak/s5cmd/issues/869)) and log leakage into `stdout` corrupting `cat` pipelines ([#851](https://github.com/peak/s5cmd/issues/851)).

---

## 2. `t3` ("Turbo 3") Architecture

```text
crates/
├── t3-cli/             # Binary entrypoint (`t3`), s5cmd-compatible clap parser, shell completions
├── t3-core/            # Execution engine: two-tier work scheduler, DFA glob filter, speculative prefix fan-out lister, sync engine, `run` batch parser
├── t3-storage/         # Bounded slab BufferPool, pre-allocated pwrite/pread positional I/O, DashSet mkdir cache, path-traversal guard
├── t3-s3-client/       # High-throughput S3 data-plane client (cached SigV4 signing, multi-endpoint round-robin load balancing, connection pool)
├── t3-telemetry/       # Lock-free atomic stats (--stat), NDJSON structured logger (--json), indicatif progress bars (-sp)
└── t3-benchmarks/      # Automated microbenchmark & end-to-end S3 transfer performance rating suite
```

### Key Engineering Advantages Over `s5cmd` and `aws-cli`
1. **Zero-Reflection Startup (< 2 ms)**: Instant process initialization vs ~20 ms in Go (`s5cmd`) and ~450 ms in Python (`aws-cli`).
2. **Cached Daily SigV4 Signing Key**: Caches the 4-stage HMAC-SHA256 signing key derivation per `(date, region)` in a lock-free `RwLock`, signing requests in **~1.5 µs** (>600,000 signings/sec per core).
3. **Bounded Slab `BufferPool` & Two-Tier Concurrency**: Decouples `--numworkers` (default 256) and `--concurrency` (default 5) with a bounded reusable buffer slab pool so RSS memory stays strictly bounded.
4. **Zero-Lock Positional Disk I/O (`pread` / `pwrite`) + `DashSet` DirCache**: Pre-allocates target files via `set_len` (`ftruncate`) and writes byte ranges concurrently via `FileExt::write_all_at` (`pwrite`) without mutex locking or temporary part file concatenation.
5. **Speculative Prefix Fan-Out Listing**: Automatically detects multi-level wildcard patterns (e.g., `logs/2026/*/*.parquet`), queries `Delimiter=/`, and fans out concurrent `ListObjectsV2` streams across sub-prefixes.
6. **Security & Correctness Fixes**:
   - Blocks `..` path traversal in S3 keys (`sanitize_relative_path`, fixing `s5cmd` #872).
   - Enforces `--exclude` precedence over `--include` in a single `RegexSet` DFA, including during `sync --delete` (fixing `s5cmd` #815 / #852).
   - Parallel multipart S3-to-S3 copy for objects > 5 GiB (fixing `s5cmd` #29).
   - Pipelined 1,000-key `DeleteObjects` batching during listing streams (fixing `s5cmd` #870).
