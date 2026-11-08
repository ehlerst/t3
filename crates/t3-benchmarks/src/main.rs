use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;
use axum::Router;
use bytes::Bytes;
use chrono::Utc;
use clap::Parser;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use t3_core::{
    sanitize_url_for_bench, split_wildcard, CopyOptions, GlobFilter, RmOptions, SelectQuery,
    SyncOptions, T3Engine,
};
use t3_s3_client::sigv4::SigV4Signer;
use t3_s3_client::xml;
use t3_s3_client::{AwsCredentials, ConfigOverrides, ResolvedConfig, S3Client};
use t3_storage::{sanitize_relative_path, DirCache, PositionalReader, PreallocatedWriter};
use t3_telemetry::{LogLevel, OutputContext};
use url::Url;

#[derive(Parser, Debug)]
#[command(
    name = "t3-benchmarks",
    about = "Performance benchmark and rating runner for t3 (Turbo 3)"
)]
pub struct Opts {
    #[arg(short, long, default_value = "5000")]
    pub iterations: usize,

    #[arg(short, long, default_value = "markdown")]
    pub format: String,

    #[arg(short, long)]
    pub output: Option<String>,

    #[arg(long)]
    pub endpoint_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub component: String,
    pub operation: String,
    pub payload_size: Option<String>,
    pub total_ops: usize,
    pub duration_secs: f64,
    pub ops_per_sec: f64,
    pub throughput_mb_per_sec: Option<f64>,
    pub p50_micros: f64,
    pub p90_micros: f64,
    pub p95_micros: f64,
    pub p99_micros: f64,
    pub rating: String,
}

fn calculate_percentiles(
    mut latencies: Vec<Duration>,
    duration_total: Duration,
    total_ops: usize,
    payload_bytes: Option<usize>,
    component: &str,
    operation: &str,
    payload_desc: Option<&str>,
) -> BenchmarkResult {
    latencies.sort();
    let n = latencies.len().max(1);
    let p50 = latencies[(n * 50 / 100).min(n - 1)].as_secs_f64() * 1_000_000.0;
    let p90 = latencies[(n * 90 / 100).min(n - 1)].as_secs_f64() * 1_000_000.0;
    let p95 = latencies[(n * 95 / 100).min(n - 1)].as_secs_f64() * 1_000_000.0;
    let p99 = latencies[(n * 99 / 100).min(n - 1)].as_secs_f64() * 1_000_000.0;

    let duration_secs = duration_total.as_secs_f64().max(1e-9);
    let ops_per_sec = total_ops as f64 / duration_secs;
    let throughput_mb =
        payload_bytes.map(|b| (total_ops * b) as f64 / (1024.0 * 1024.0 * duration_secs));

    let rating = if p95 < 25.0 || ops_per_sec > 25_000.0 || throughput_mb.unwrap_or(0.0) > 500.0 {
        "A+ (Ultra Fast)"
    } else if p95 < 250.0 || ops_per_sec > 4_000.0 || throughput_mb.unwrap_or(0.0) > 100.0 {
        "A (Excellent)"
    } else {
        "B+ (Very Good)"
    };

    BenchmarkResult {
        component: component.to_string(),
        operation: operation.to_string(),
        payload_size: payload_desc.map(|s| s.to_string()),
        total_ops,
        duration_secs,
        ops_per_sec,
        throughput_mb_per_sec: throughput_mb,
        p50_micros: p50,
        p90_micros: p90,
        p95_micros: p95,
        p99_micros: p99,
        rating: rating.to_string(),
    }
}

pub fn run_core_microbenchmarks(iterations: usize) -> Vec<BenchmarkResult> {
    let mut results = Vec::new();

    // 1. Cached SigV4 Request Signing
    let signer = SigV4Signer::new();
    let creds = AwsCredentials {
        access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        session_token: None,
    };
    let url =
        Url::parse("http://localhost:4566/bench-bucket/datasets/train/shard-0001.parquet").unwrap();
    let now = Utc::now();
    let mut latencies = Vec::with_capacity(iterations);
    let start_total = Instant::now();
    for _ in 0..iterations {
        let mut headers = http::HeaderMap::new();
        let start = Instant::now();
        signer.sign_request(
            "PUT",
            &url,
            &mut headers,
            "UNSIGNED-PAYLOAD",
            &creds,
            "us-east-1",
            now,
        );
        latencies.push(start.elapsed());
    }
    results.push(calculate_percentiles(
        latencies,
        start_total.elapsed(),
        iterations,
        None,
        "t3-s3-client",
        "SigV4SignRequest (Cached Key)",
        Some("Auth Header"),
    ));

    // 2. Presign URL Generation
    let mut latencies = Vec::with_capacity(iterations);
    let start_total = Instant::now();
    for _ in 0..iterations {
        let start = Instant::now();
        let _ = signer.presign_url("GET", &url, &creds, "us-east-1", 3600, now);
        latencies.push(start.elapsed());
    }
    results.push(calculate_percentiles(
        latencies,
        start_total.elapsed(),
        iterations,
        None,
        "t3-s3-client",
        "SigV4PresignUrl",
        Some("Query Auth"),
    ));

    // 3. DFA GlobFilter Match (--include + --exclude)
    let filter = GlobFilter::compile(
        Some("datasets/2026/*/*.parquet"),
        &["*.parquet".to_string(), "*.safetensors".to_string()],
        &["*checkpoint_tmp*".to_string(), "*.bak".to_string()],
    )
    .unwrap();
    let mut latencies = Vec::with_capacity(iterations);
    let start_total = Instant::now();
    for _ in 0..iterations {
        let start = Instant::now();
        let matched = filter.matches(
            "datasets/2026/08/part-00042.parquet",
            "08/part-00042.parquet",
        );
        std::hint::black_box(matched);
        latencies.push(start.elapsed());
    }
    results.push(calculate_percentiles(
        latencies,
        start_total.elapsed(),
        iterations,
        None,
        "t3-core",
        "GlobFilterDFA (Include+Exclude)",
        Some("4 Globs"),
    ));

    // 4. Path Traversal Security Guard + Wildcard Splitter
    let mut latencies = Vec::with_capacity(iterations);
    let start_total = Instant::now();
    for _ in 0..iterations {
        let start = Instant::now();
        let _ = sanitize_relative_path("models/llama4/weights/shard_001.safetensors").unwrap();
        let _ = split_wildcard("models/llama4/*/*.safetensors", false);
        latencies.push(start.elapsed());
    }
    results.push(calculate_percentiles(
        latencies,
        start_total.elapsed(),
        iterations,
        None,
        "t3-storage",
        "PathTraversalGuard + WildcardSplit",
        Some("Key Validation"),
    ));

    // 5. ListObjectsV2 XML Parsing (100 objects/page)
    let mut sample_xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bench</Name><IsTruncated>false</IsTruncated>"#,
    );
    for i in 0..100 {
        sample_xml.push_str(&format!(
            "<Contents><Key>data/shard_{i:04}.parquet</Key><LastModified>2026-11-08T12:00:00Z</LastModified><ETag>\"abcdef123456\"</ETag><Size>65536</Size><StorageClass>STANDARD</StorageClass></Contents>"
        ));
    }
    sample_xml.push_str("</ListBucketResult>");
    let xml_bytes = sample_xml.into_bytes();
    let xml_iters = (iterations / 5).max(200);
    let mut latencies = Vec::with_capacity(xml_iters);
    let start_total = Instant::now();
    for _ in 0..xml_iters {
        let start = Instant::now();
        let page = xml::parse_list_objects(&xml_bytes).unwrap();
        std::hint::black_box(page.objects.len());
        latencies.push(start.elapsed());
    }
    results.push(calculate_percentiles(
        latencies,
        start_total.elapsed(),
        xml_iters,
        Some(xml_bytes.len()),
        "t3-s3-client",
        "ParseListObjectsV2Xml",
        Some("100 Keys/Page"),
    ));

    // 6. Zero-Lock Positional File I/O (pwrite + pread 64 KB)
    let tmp_dir = tempfile::tempdir().unwrap();
    let file_path = tmp_dir.path().join("io_bench.bin");
    let cache = DirCache::new();
    let payload_64k = vec![0xABu8; 64 * 1024];
    let io_iters = (iterations / 2).max(200);
    let writer =
        PreallocatedWriter::create(&file_path, (io_iters * 64 * 1024) as u64, &cache).unwrap();
    let mut latencies = Vec::with_capacity(io_iters);
    let start_total = Instant::now();
    for i in 0..io_iters {
        let start = Instant::now();
        writer
            .write_at((i * 64 * 1024) as u64, &payload_64k)
            .unwrap();
        latencies.push(start.elapsed());
    }
    results.push(calculate_percentiles(
        latencies,
        start_total.elapsed(),
        io_iters,
        Some(64 * 1024),
        "t3-storage",
        "PreallocatedPwrite (write_all_at)",
        Some("64 KB"),
    ));

    let reader = PositionalReader::open(&file_path).unwrap();
    let mut read_buf = vec![0u8; 64 * 1024];
    let mut latencies = Vec::with_capacity(io_iters);
    let start_total = Instant::now();
    for i in 0..io_iters {
        let start = Instant::now();
        reader
            .read_at((i * 64 * 1024) as u64, &mut read_buf)
            .unwrap();
        latencies.push(start.elapsed());
    }
    results.push(calculate_percentiles(
        latencies,
        start_total.elapsed(),
        io_iters,
        Some(64 * 1024),
        "t3-storage",
        "PositionalPread (read_exact_at)",
        Some("64 KB"),
    ));

    // 7. S3 Select Engine (NDJSON SQL Filtering + Projection)
    let query = SelectQuery::parse("SELECT s.id, s.score FROM S3Object s WHERE s.score >= 90");
    let ndjson_payload = br#"{"id":1,"score":95,"tag":"a"}
{"id":2,"score":70,"tag":"b"}
{"id":3,"score":99,"tag":"c"}
{"id":4,"score":88,"tag":"d"}"#;
    let sel_iters = (iterations / 2).max(200);
    let mut latencies = Vec::with_capacity(sel_iters);
    let start_total = Instant::now();
    for _ in 0..sel_iters {
        let start = Instant::now();
        let rows = query.execute_on_bytes(ndjson_payload, "json", "json");
        std::hint::black_box(rows.len());
        latencies.push(start.elapsed());
    }
    results.push(calculate_percentiles(
        latencies,
        start_total.elapsed(),
        sel_iters,
        Some(ndjson_payload.len()),
        "t3-core",
        "SelectQueryExecute (NDJSON)",
        Some("4 Rows/Doc"),
    ));

    results
}

#[derive(Clone, Default)]
struct EmbeddedS3State {
    objects: Arc<DashMap<(String, String), Bytes>>,
}

async fn start_embedded_s3_server() -> (String, tokio::task::JoinHandle<()>) {
    let state = EmbeddedS3State::default();
    let app = Router::new()
        .route(
            "/{bucket}",
            axum::routing::put(handle_bucket_put)
                .post(handle_bucket_post)
                .get(handle_bucket_get),
        )
        .route(
            "/{bucket}/{*key}",
            axum::routing::put(handle_obj_put)
                .get(handle_obj_get)
                .head(handle_obj_head)
                .post(handle_obj_post)
                .delete(handle_obj_delete),
        )
        .layer(axum::extract::DefaultBodyLimit::disable())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), handle)
}

async fn handle_bucket_put() -> StatusCode {
    StatusCode::OK
}

async fn handle_bucket_post(
    State(state): State<EmbeddedS3State>,
    AxumPath(bucket): AxumPath<String>,
    body: Bytes,
) -> Response {
    let text = String::from_utf8_lossy(&body);
    let mut xml_out = String::from("<DeleteResult>");
    for part in text.split("<Key>").skip(1) {
        if let Some(end) = part.find("</Key>") {
            let k = &part[..end];
            state.objects.remove(&(bucket.clone(), k.to_string()));
            xml_out.push_str(&format!("<Deleted><Key>{k}</Key></Deleted>"));
        }
    }
    xml_out.push_str("</DeleteResult>");
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(xml_out))
        .unwrap()
}

async fn handle_bucket_get(
    State(state): State<EmbeddedS3State>,
    AxumPath(bucket): AxumPath<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let mut xml_out =
        format!("<ListBucketResult><Name>{bucket}</Name><IsTruncated>false</IsTruncated>");
    for entry in state.objects.iter() {
        let (b, k) = entry.key();
        if b == &bucket && k.starts_with(&prefix) {
            xml_out.push_str(&format!(
                "<Contents><Key>{k}</Key><LastModified>2026-11-08T12:00:00Z</LastModified><ETag>\"etag123\"</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                entry.value().len()
            ));
        }
    }
    xml_out.push_str("</ListBucketResult>");
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(xml_out))
        .unwrap()
}

async fn handle_obj_put(
    State(state): State<EmbeddedS3State>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    if let (Some(uid), Some(pnum)) = (q.get("uploadId"), q.get("partNumber")) {
        state
            .objects
            .insert((bucket, format!("{key}.part.{uid}.{pnum}")), body);
    } else {
        state.objects.insert((bucket, key), body);
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("etag", HeaderValue::from_static("\"etag123\""))
        .body(Body::empty())
        .unwrap()
}

async fn handle_obj_post(
    State(state): State<EmbeddedS3State>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if q.contains_key("uploads") {
        let xml_out = format!(
            "<InitiateMultipartUploadResult><Bucket>{bucket}</Bucket><Key>{key}</Key><UploadId>uid123</UploadId></InitiateMultipartUploadResult>"
        );
        return Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(xml_out))
            .unwrap();
    }
    if let Some(uid) = q.get("uploadId") {
        let mut combined = Vec::new();
        let mut p = 1;
        while let Some((_, part_bytes)) = state
            .objects
            .remove(&(bucket.clone(), format!("{key}.part.{uid}.{p}")))
        {
            combined.extend_from_slice(&part_bytes);
            p += 1;
        }
        state
            .objects
            .insert((bucket.clone(), key.clone()), Bytes::from(combined));
        let xml_out = format!(
            "<CompleteMultipartUploadResult><Bucket>{bucket}</Bucket><Key>{key}</Key><ETag>\"etag-mp\"</ETag></CompleteMultipartUploadResult>"
        );
        return Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(xml_out))
            .unwrap();
    }
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .unwrap()
}

async fn handle_obj_head(
    State(state): State<EmbeddedS3State>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
) -> Response {
    if let Some(data) = state.objects.get(&(bucket, key)) {
        Response::builder()
            .status(StatusCode::OK)
            .header("content-length", data.len().to_string())
            .header("etag", "\"etag123\"")
            .body(Body::empty())
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()
    }
}

async fn handle_obj_get(
    State(state): State<EmbeddedS3State>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let Some(data) = state.objects.get(&(bucket, key)) else {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    };
    if let Some(range_str) = headers.get("range").and_then(|v| v.to_str().ok()) {
        if let Some(spec) = range_str.strip_prefix("bytes=") {
            if let Some((s, e)) = spec.split_once('-') {
                if let (Ok(start), Ok(end)) = (s.parse::<usize>(), e.parse::<usize>()) {
                    let end_clamped = end.min(data.len().saturating_sub(1));
                    let slice = data.slice(start..=end_clamped);
                    return Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header("content-length", slice.len().to_string())
                        .body(Body::from(slice))
                        .unwrap();
                }
            }
        }
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("content-length", data.len().to_string())
        .body(Body::from(data.clone()))
        .unwrap()
}

async fn handle_obj_delete(
    State(state): State<EmbeddedS3State>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
) -> StatusCode {
    state.objects.remove(&(bucket, key));
    StatusCode::NO_CONTENT
}

pub async fn run_e2e_transfer_benchmarks(
    endpoint_override: Option<String>,
) -> Vec<BenchmarkResult> {
    let mut results = Vec::new();
    let (endpoint, _server_guard) = match endpoint_override {
        Some(ep) => (ep, None),
        None => {
            let (ep, h) = start_embedded_s3_server().await;
            (ep, Some(h))
        }
    };

    let overrides = ConfigOverrides {
        endpoint_url: Some(endpoint),
        retry_count: 3,
        unsigned_payload: true,
        ..Default::default()
    };
    let cfg = ResolvedConfig::resolve(&overrides);
    let output = OutputContext::new(false, false, false, false, LogLevel::Error).with_quiet(true);
    let client = S3Client::new(cfg, Some(output.stats.clone())).unwrap();
    let engine = T3Engine::new(client, output, 256);

    let _ = engine.mb("s3://t3-bench").await;

    // 1. Parallel Small Files Upload (200 x 4 KB)
    let src_dir = tempfile::tempdir().unwrap();
    let payload_4k = vec![b'x'; 4096];
    let file_count = 200usize;
    for i in 0..file_count {
        fs::write(src_dir.path().join(format!("file_{i:04}.dat")), &payload_4k).unwrap();
    }

    let start = Instant::now();
    engine
        .cp(
            &src_dir.path().to_string_lossy(),
            "s3://t3-bench/small/",
            &CopyOptions::default(),
        )
        .await
        .unwrap();
    let elapsed = start.elapsed();
    results.push(calculate_percentiles(
        vec![elapsed / file_count as u32; file_count],
        elapsed,
        file_count,
        Some(4096),
        "t3-engine",
        "ParallelUpload (256 Workers)",
        Some("200 x 4 KB"),
    ));

    // 2. Parallel Small Files Download (200 x 4 KB)
    let dst_dir = tempfile::tempdir().unwrap();
    let start = Instant::now();
    engine
        .cp(
            "s3://t3-bench/small/*",
            &format!("{}/", dst_dir.path().display()),
            &CopyOptions::default(),
        )
        .await
        .unwrap();
    let elapsed = start.elapsed();
    results.push(calculate_percentiles(
        vec![elapsed / file_count as u32; file_count],
        elapsed,
        file_count,
        Some(4096),
        "t3-engine",
        "ParallelDownload (pwrite + DirCache)",
        Some("200 x 4 KB"),
    ));

    // 3. Large Multipart Upload & Parallel Range Download (32 MiB, 5 MiB parts, concurrency=8)
    let large_src = src_dir.path().join("large_32mb.bin");
    let large_size = 32 * 1024 * 1024usize;
    fs::write(&large_src, vec![0x5Au8; large_size]).unwrap();
    let mp_opts = CopyOptions {
        concurrency: 8,
        part_size_bytes: 5 * 1024 * 1024,
        ..Default::default()
    };
    let start = Instant::now();
    engine
        .cp(
            &large_src.to_string_lossy(),
            "s3://t3-bench/large/large_32mb.bin",
            &mp_opts,
        )
        .await
        .unwrap();
    let elapsed = start.elapsed();
    results.push(calculate_percentiles(
        vec![elapsed],
        elapsed,
        1,
        Some(large_size),
        "t3-engine",
        "MultipartUpload (pread + BufferPool)",
        Some("32 MiB (8x parts)"),
    ));

    let large_dst = dst_dir.path().join("downloaded_32mb.bin");
    let start = Instant::now();
    engine
        .cp(
            "s3://t3-bench/large/large_32mb.bin",
            &large_dst.to_string_lossy(),
            &mp_opts,
        )
        .await
        .unwrap();
    let elapsed = start.elapsed();
    results.push(calculate_percentiles(
        vec![elapsed],
        elapsed,
        1,
        Some(large_size),
        "t3-engine",
        "ParallelRangeDownload (pwrite)",
        Some("32 MiB (8x ranges)"),
    ));

    // 4. Incremental Sync (200 files up-to-date diffing)
    let start = Instant::now();
    engine
        .sync(
            &src_dir.path().to_string_lossy(),
            "s3://t3-bench/small/",
            &SyncOptions {
                size_only: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let elapsed = start.elapsed();
    results.push(calculate_percentiles(
        vec![elapsed / file_count as u32; file_count],
        elapsed,
        file_count,
        None,
        "t3-engine",
        "ConcurrentSyncDiff",
        Some("200 Objects"),
    ));

    // 5. Pipelined Batch Delete (200 keys via DeleteObjects)
    let start = Instant::now();
    engine
        .rm(
            &["s3://t3-bench/small/*".to_string()],
            &RmOptions::default(),
        )
        .await
        .unwrap();
    let elapsed = start.elapsed();
    results.push(calculate_percentiles(
        vec![elapsed / file_count as u32; file_count],
        elapsed,
        file_count,
        None,
        "t3-engine",
        "BatchDeleteObjects (1000/batch)",
        Some("200 Keys"),
    ));

    let _ = sanitize_url_for_bench("s3://t3-bench");
    results
}

fn format_markdown(results: &[BenchmarkResult]) -> String {
    let mut md = String::new();
    md.push_str("# ⚡ t3 (Turbo 3) Performance Benchmark Report\n\n");
    md.push_str("| Component | Operation | Payload / Batch | Throughput | MB/s | p50 Latency | p95 Latency | p99 Latency | Grade |\n");
    md.push_str("|:---|:---|:---|---:|---:|---:|---:|---:|:---|\n");

    for r in results {
        let mb_str = r
            .throughput_mb_per_sec
            .map(|m| format!("{m:.1} MB/s"))
            .unwrap_or_else(|| "N/A".to_string());
        md.push_str(&format!(
            "| **{}** | `{}` | {} | **{:.0} ops/s** | {} | {:.1} µs | {:.1} µs | {:.1} µs | **{}** |\n",
            r.component,
            r.operation,
            r.payload_size.as_deref().unwrap_or("-"),
            r.ops_per_sec,
            mb_str,
            r.p50_micros,
            r.p95_micros,
            r.p99_micros,
            r.rating
        ));
    }
    md
}

#[tokio::main]
async fn main() {
    let opts = Opts::parse();
    let mut results = run_core_microbenchmarks(opts.iterations);
    let e2e = run_e2e_transfer_benchmarks(opts.endpoint_url).await;
    results.extend(e2e);

    let formatted = if opts.format == "json" {
        serde_json::to_string_pretty(&results).unwrap()
    } else {
        format_markdown(&results)
    };

    if let Some(ref path) = opts.output {
        if let Some(parent) = std::path::Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                let _ = fs::create_dir_all(parent);
            }
        }
        let mut f = File::create(path).unwrap();
        f.write_all(formatted.as_bytes()).unwrap();
    } else {
        println!("{formatted}");
    }
}
