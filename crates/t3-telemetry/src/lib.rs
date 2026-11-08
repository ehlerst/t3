use chrono::{DateTime, Utc};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Error,
}

impl LogLevel {
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "trace" => Self::Trace,
            "debug" => Self::Debug,
            "error" | "err" => Self::Error,
            _ => Self::Info,
        }
    }
}

#[derive(Debug)]
pub struct TelemetryStats {
    start_time: Instant,
    pub objects_transferred: AtomicU64,
    pub objects_listed: AtomicU64,
    pub objects_deleted: AtomicU64,
    pub parts_transferred: AtomicU64,
    pub bytes_transferred: AtomicU64,
    pub ops_succeeded: AtomicU64,
    pub ops_failed: AtomicU64,
    pub retries: AtomicU64,
}

impl Default for TelemetryStats {
    fn default() -> Self {
        Self::new()
    }
}

impl TelemetryStats {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            objects_transferred: AtomicU64::new(0),
            objects_listed: AtomicU64::new(0),
            objects_deleted: AtomicU64::new(0),
            parts_transferred: AtomicU64::new(0),
            bytes_transferred: AtomicU64::new(0),
            ops_succeeded: AtomicU64::new(0),
            ops_failed: AtomicU64::new(0),
            retries: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn record_transfer(&self, bytes: u64, parts: u64) {
        self.objects_transferred.fetch_add(1, Ordering::Relaxed);
        self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);
        self.parts_transferred.fetch_add(parts, Ordering::Relaxed);
        self.ops_succeeded.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_listed(&self, count: u64) {
        self.objects_listed.fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_deleted(&self, count: u64) {
        self.objects_deleted.fetch_add(count, Ordering::Relaxed);
        self.ops_succeeded.fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_op_success(&self) {
        self.ops_succeeded.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_op_error(&self) {
        self.ops_failed.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        let elapsed = self.start_time.elapsed();
        let secs = elapsed.as_secs_f64().max(1e-9);
        let bytes = self.bytes_transferred.load(Ordering::Relaxed);
        let ops_ok = self.ops_succeeded.load(Ordering::Relaxed);
        let ops_err = self.ops_failed.load(Ordering::Relaxed);
        let total_ops = ops_ok + ops_err;

        StatsSnapshot {
            elapsed_ms: elapsed.as_secs_f64() * 1000.0,
            objects_transferred: self.objects_transferred.load(Ordering::Relaxed),
            objects_listed: self.objects_listed.load(Ordering::Relaxed),
            objects_deleted: self.objects_deleted.load(Ordering::Relaxed),
            parts_transferred: self.parts_transferred.load(Ordering::Relaxed),
            bytes_transferred: bytes,
            ops_succeeded: ops_ok,
            ops_failed: ops_err,
            retries: self.retries.load(Ordering::Relaxed),
            ops_per_sec: total_ops as f64 / secs,
            mib_per_sec: (bytes as f64 / (1024.0 * 1024.0)) / secs,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsSnapshot {
    pub elapsed_ms: f64,
    pub objects_transferred: u64,
    pub objects_listed: u64,
    pub objects_deleted: u64,
    pub parts_transferred: u64,
    pub bytes_transferred: u64,
    pub ops_succeeded: u64,
    pub ops_failed: u64,
    pub retries: u64,
    pub ops_per_sec: f64,
    pub mib_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredEvent {
    pub operation: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone)]
pub struct OutputContext {
    pub json_mode: bool,
    pub stat_mode: bool,
    pub dry_run: bool,
    pub show_progress: bool,
    pub quiet: bool,
    pub log_level: LogLevel,
    pub stats: Arc<TelemetryStats>,
    multi_progress: Option<Arc<MultiProgress>>,
}

impl Default for OutputContext {
    fn default() -> Self {
        Self::new(false, false, false, false, LogLevel::Info)
    }
}

impl OutputContext {
    pub fn new(
        json_mode: bool,
        stat_mode: bool,
        dry_run: bool,
        show_progress: bool,
        log_level: LogLevel,
    ) -> Self {
        let multi_progress = if show_progress && !json_mode {
            Some(Arc::new(MultiProgress::new()))
        } else {
            None
        };
        Self {
            json_mode,
            stat_mode,
            dry_run,
            show_progress,
            quiet: false,
            log_level,
            stats: Arc::new(TelemetryStats::new()),
            multi_progress,
        }
    }

    pub fn with_quiet(mut self, quiet: bool) -> Self {
        self.quiet = quiet;
        self
    }

    pub fn create_progress_bar(&self, total_bytes: u64, label: &str) -> Option<ProgressBar> {
        let mp = self.multi_progress.as_ref()?;
        let pb = mp.add(ProgressBar::new(total_bytes));
        let style = ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) {msg}",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("#>-");
        pb.set_style(style);
        pb.set_message(label.to_string());
        Some(pb)
    }

    pub fn emit_action(&self, op: &str, src: &str, dst: Option<&str>, size: Option<u64>) {
        if self.quiet {
            return;
        }
        if self.json_mode {
            let ev = StructuredEvent {
                operation: op.to_string(),
                success: true,
                source: Some(src.to_string()),
                destination: dst.map(|s| s.to_string()),
                key: None,
                size,
                etag: None,
                last_modified: None,
                storage_class: None,
                message: if self.dry_run {
                    Some("dry-run".to_string())
                } else {
                    None
                },
                error: None,
            };
            if let Ok(line) = serde_json::to_string(&ev) {
                let mut out = io::stdout().lock();
                let _ = writeln!(out, "{line}");
            }
        } else {
            let prefix = if self.dry_run { "(dry-run) " } else { "" };
            let mut out = io::stdout().lock();
            match dst {
                Some(d) => {
                    let _ = writeln!(out, "{prefix}{op} {src} {d}");
                }
                None => {
                    let _ = writeln!(out, "{prefix}{op} {src}");
                }
            }
        }
    }

    pub fn emit_error(&self, op: &str, target: &str, err: &str) {
        self.stats.record_op_error();
        if self.json_mode {
            let ev = StructuredEvent {
                operation: op.to_string(),
                success: false,
                source: Some(target.to_string()),
                destination: None,
                key: None,
                size: None,
                etag: None,
                last_modified: None,
                storage_class: None,
                message: None,
                error: Some(err.to_string()),
            };
            if let Ok(line) = serde_json::to_string(&ev) {
                let mut out = io::stderr().lock();
                let _ = writeln!(out, "{line}");
            }
        } else {
            let mut out = io::stderr().lock();
            let _ = writeln!(out, "ERROR \"{op} {target}\": {err}");
        }
    }

    pub fn log_debug(&self, msg: &str) {
        if matches!(self.log_level, LogLevel::Debug | LogLevel::Trace) {
            let mut err = io::stderr().lock();
            let _ = writeln!(err, "DEBUG {msg}");
        }
    }

    pub fn log_trace(&self, msg: &str) {
        if matches!(self.log_level, LogLevel::Trace) {
            let mut err = io::stderr().lock();
            let _ = writeln!(err, "TRACE {msg}");
        }
    }

    pub fn print_stats(&self) {
        if !self.stat_mode {
            return;
        }
        let snap = self.stats.snapshot();
        if self.json_mode {
            let val = serde_json::json!({
                "operation": "stat",
                "stats": snap,
            });
            let mut out = io::stdout().lock();
            let _ = writeln!(out, "{val}");
        } else {
            let mut out = io::stdout().lock();
            let _ = writeln!(
                out,
                "\nOperation      Total       Transferred   Deleted     Listed      Retries     Errors      Duration    Throughput   Rate"
            );
            let _ = writeln!(
                out,
                "{:<14} {:<11} {:<13} {:<11} {:<11} {:<11} {:<11} {:<11} {:<12} {:.0} ops/s",
                "Summary",
                snap.ops_succeeded + snap.ops_failed,
                format_bytes(snap.bytes_transferred, true),
                snap.objects_deleted,
                snap.objects_listed,
                snap.retries,
                snap.ops_failed,
                format_duration(Duration::from_secs_f64(snap.elapsed_ms / 1000.0)),
                format!("{:.2} MiB/s", snap.mib_per_sec),
                snap.ops_per_sec
            );
        }
    }
}

pub fn format_bytes(bytes: u64, humanize: bool) -> String {
    if !humanize {
        return format!("{bytes:>12}");
    }
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut val = bytes as f64;
    let mut idx = 0;
    while val >= 1024.0 && idx + 1 < UNITS.len() {
        val /= 1024.0;
        idx += 1;
    }
    format!("{val:.1} {}", UNITS[idx])
}

pub fn format_duration(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1000.0 {
        format!("{ms:.1}ms")
    } else {
        format!("{:.2}s", d.as_secs_f64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_recording_and_snapshot() {
        let stats = TelemetryStats::new();
        stats.record_transfer(1024 * 1024, 2);
        stats.record_listed(15);
        stats.record_deleted(3);
        stats.record_retry();

        let snap = stats.snapshot();
        assert_eq!(snap.objects_transferred, 1);
        assert_eq!(snap.bytes_transferred, 1024 * 1024);
        assert_eq!(snap.parts_transferred, 2);
        assert_eq!(snap.objects_listed, 15);
        assert_eq!(snap.objects_deleted, 3);
        assert_eq!(snap.retries, 1);
        assert_eq!(snap.ops_succeeded, 4);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(512, true), "512 B");
        assert_eq!(format_bytes(2048, true), "2.0 KiB");
        assert_eq!(format_bytes(5 * 1024 * 1024, true), "5.0 MiB");
    }
}
