# ⚡ t3 (Turbo 3)

**Blazing-fast parallel S3 CLI and drop-in `s5cmd` replacement in Rust — sister project to [RustStack](https://github.com/ehlerst/ruststack).**

## Quick Start

```bash
docker run --rm ehlers320/t3:latest --version

# Connect to local RustStack or custom S3 endpoint
docker run --rm --network host \
  -e AWS_ACCESS_KEY_ID=test \
  -e AWS_SECRET_ACCESS_KEY=test \
  ehlers320/t3:latest --endpoint-url http://localhost:4566 ls
```

## Key Features
- **Drop-In `s5cmd` Replacement**: Supports all 15 subcommands (`cp`, `mv`, `sync`, `ls`, `rm`, `run`, `cat`, `pipe`, `head`, `du`, `mb`, `rb`, `presign`, `select`, `bucket-version`, `version`).
- **Two-Tier Concurrency & Bounded Buffer Pool**: `--numworkers 256` object parallelism + `--concurrency 5` part parallelism with reusable memory slabs.
- **Zero-Lock Positional Disk I/O**: Pre-allocated `ftruncate` + concurrent `pwrite` (`write_all_at`) and `pread` (`read_exact_at`).
- **Fixes Unpatched `s5cmd` Bugs**:
  - Supports > 5 GiB S3-to-S3 multipart copies (`#29`).
  - Blocks `../` path traversal on malicious S3 keys (`#872`).
  - Honors `--exclude` / `--include` filters during `sync --delete` (`#815`, `#852`).
  - Pipelined concurrent 1,000-key `DeleteObjects` batching (`#870`).
