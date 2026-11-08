use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;
use axum::Router;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use t3_core::{CopyOptions, DuOptions, LsOptions, SyncOptions, T3Engine};
use t3_s3_client::{ConfigOverrides, ResolvedConfig, S3Client};
use t3_telemetry::{LogLevel, OutputContext};

#[derive(Clone, Default)]
struct MockS3State {
    buckets: Arc<DashMap<String, String>>,
    objects: Arc<DashMap<(String, String), Bytes>>,
    fail_next_puts: Arc<AtomicUsize>,
}

async fn spawn_mock_s3() -> (String, MockS3State, tokio::task::JoinHandle<()>) {
    let state = MockS3State::default();
    let app = Router::new()
        .route(
            "/",
            axum::routing::get(|State(st): State<MockS3State>| async move {
                let mut xml = String::from("<ListAllMyBucketsResult><Buckets>");
                for b in st.buckets.iter() {
                    xml.push_str(&format!(
                        "<Bucket><Name>{}</Name><CreationDate>2026-11-08T12:00:00Z</CreationDate></Bucket>",
                        b.key()
                    ));
                }
                xml.push_str("</Buckets></ListAllMyBucketsResult>");
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from(xml))
                    .unwrap()
            }),
        )
        .route(
            "/{bucket}",
            axum::routing::put(bucket_put)
                .delete(bucket_delete)
                .head(bucket_head)
                .post(bucket_post)
                .get(bucket_get),
        )
        .route(
            "/{bucket}/{*key}",
            axum::routing::put(object_put)
                .get(object_get)
                .head(object_head)
                .post(object_post)
                .delete(object_delete),
        )
        .layer(axum::extract::DefaultBodyLimit::disable())
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), state, handle)
}

async fn bucket_put(
    State(state): State<MockS3State>,
    AxumPath(bucket): AxumPath<String>,
    Query(q): Query<HashMap<String, String>>,
    body: Bytes,
) -> StatusCode {
    if q.contains_key("versioning") {
        let txt = String::from_utf8_lossy(&body);
        let status = if txt.contains("Enabled") {
            "Enabled"
        } else {
            "Suspended"
        };
        state.buckets.insert(bucket, status.to_string());
    } else {
        state.buckets.insert(bucket, "Unversioned".to_string());
    }
    StatusCode::OK
}

async fn bucket_delete(
    State(state): State<MockS3State>,
    AxumPath(bucket): AxumPath<String>,
) -> StatusCode {
    state.buckets.remove(&bucket);
    StatusCode::NO_CONTENT
}

async fn bucket_head(
    State(state): State<MockS3State>,
    AxumPath(bucket): AxumPath<String>,
) -> StatusCode {
    if state.buckets.contains_key(&bucket) {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn bucket_post(
    State(state): State<MockS3State>,
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

async fn bucket_get(
    State(state): State<MockS3State>,
    AxumPath(bucket): AxumPath<String>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    if q.contains_key("versioning") {
        let st = state
            .buckets
            .get(&bucket)
            .map(|v| v.clone())
            .unwrap_or_else(|| "Unversioned".to_string());
        let xml =
            format!("<VersioningConfiguration><Status>{st}</Status></VersioningConfiguration>");
        return Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(xml))
            .unwrap();
    }
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let mut keys: Vec<(String, usize)> = state
        .objects
        .iter()
        .filter_map(|e| {
            let (b, k) = e.key();
            if b == &bucket && k.starts_with(&prefix) && !k.contains(".part.") {
                Some((k.clone(), e.value().len()))
            } else {
                None
            }
        })
        .collect();
    keys.sort_by(|a, b| a.0.cmp(&b.0));

    let mut xml_out =
        format!("<ListBucketResult><Name>{bucket}</Name><IsTruncated>false</IsTruncated>");
    for (k, sz) in keys {
        xml_out.push_str(&format!(
            "<Contents><Key>{k}</Key><LastModified>2026-11-08T12:00:00Z</LastModified><ETag>\"etag\"</ETag><Size>{sz}</Size><StorageClass>STANDARD</StorageClass></Contents>"
        ));
    }
    xml_out.push_str("</ListBucketResult>");
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(xml_out))
        .unwrap()
}

async fn object_put(
    State(state): State<MockS3State>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let prev = state.fail_next_puts.load(Ordering::SeqCst);
    if prev > 0 {
        state.fail_next_puts.fetch_sub(1, Ordering::SeqCst);
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::from(
                "<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>",
            ))
            .unwrap();
    }

    if let (Some(uid), Some(pnum)) = (q.get("uploadId"), q.get("partNumber")) {
        state
            .objects
            .insert((bucket, format!("{key}.part.{uid}.{pnum}")), body);
        return Response::builder()
            .status(StatusCode::OK)
            .header("etag", HeaderValue::from_static("\"part-etag\""))
            .body(Body::empty())
            .unwrap();
    }

    if let Some(copy_src) = headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
    {
        let trimmed = copy_src.trim_start_matches('/');
        let mut sp = trimmed.splitn(2, '/');
        let sb = sp.next().unwrap_or("");
        let sk = sp.next().unwrap_or("");
        if let Some(src_bytes) = state.objects.get(&(sb.to_string(), sk.to_string())) {
            let cloned = src_bytes.clone();
            drop(src_bytes);
            state.objects.insert((bucket, key), cloned);
            return Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(
                    "<CopyObjectResult><LastModified>2026-11-08T12:00:00Z</LastModified><ETag>\"copied-etag\"</ETag></CopyObjectResult>",
                ))
                .unwrap();
        }
    }

    state.objects.insert((bucket, key), body);
    Response::builder()
        .status(StatusCode::OK)
        .header("etag", HeaderValue::from_static("\"etag123\""))
        .body(Body::empty())
        .unwrap()
}

async fn object_post(
    State(state): State<MockS3State>,
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

async fn object_head(
    State(state): State<MockS3State>,
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

async fn object_get(
    State(state): State<MockS3State>,
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

async fn object_delete(
    State(state): State<MockS3State>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
) -> StatusCode {
    state.objects.remove(&(bucket, key));
    StatusCode::NO_CONTENT
}

#[tokio::test]
async fn test_full_s3_lifecycle_multipart_sync_and_security() {
    let (endpoint, mock_state, _handle) = spawn_mock_s3().await;

    let overrides = ConfigOverrides {
        endpoint_url: Some(endpoint),
        retry_count: 5,
        unsigned_payload: true,
        ..Default::default()
    };
    let cfg = ResolvedConfig::resolve(&overrides);
    let output = OutputContext::new(true, false, false, false, LogLevel::Error);
    let client = S3Client::new(cfg, Some(output.stats.clone())).unwrap();
    let engine = T3Engine::new(client, output.clone(), 64);

    // 1. mb & bucket-version
    engine.mb("s3://test-bucket").await.unwrap();
    engine
        .bucket_version("s3://test-bucket", Some("Enabled"))
        .await
        .unwrap();
    engine
        .bucket_version("s3://test-bucket", None)
        .await
        .unwrap();

    // 2. Chaos retry verification: inject two 503 SlowDown failures and ensure cp succeeds with retries
    mock_state.fail_next_puts.store(2, Ordering::SeqCst);
    let tmp_src = tempfile::tempdir().unwrap();
    fs::write(tmp_src.path().join("hello.txt"), b"hello world").unwrap();
    fs::write(tmp_src.path().join("skip.tmp"), b"should be excluded").unwrap();

    let cp_opts = CopyOptions {
        excludes: vec!["*.tmp".to_string()],
        ..Default::default()
    };
    engine
        .cp(
            &tmp_src.path().to_string_lossy(),
            "s3://test-bucket/dir/",
            &cp_opts,
        )
        .await
        .unwrap();

    assert!(output.stats.snapshot().retries >= 2);
    assert!(mock_state
        .objects
        .contains_key(&("test-bucket".to_string(), "dir/hello.txt".to_string())));
    assert!(!mock_state
        .objects
        .contains_key(&("test-bucket".to_string(), "dir/skip.tmp".to_string())));

    // 3. Large Multipart Upload (12 MiB with 5 MiB part size -> 3 parts) & Multipart S3-to-S3 Copy & Parallel Range Download
    let large_path = tmp_src.path().join("large.bin");
    let large_data: Vec<u8> = (0..(12 * 1024 * 1024)).map(|i| (i % 251) as u8).collect();
    fs::write(&large_path, &large_data).unwrap();

    let mp_opts = CopyOptions {
        concurrency: 4,
        part_size_bytes: 5 * 1024 * 1024,
        ..Default::default()
    };
    engine
        .cp(
            &large_path.to_string_lossy(),
            "s3://test-bucket/mp/large.bin",
            &mp_opts,
        )
        .await
        .unwrap();

    // S3-to-S3 multipart copy
    engine
        .cp(
            "s3://test-bucket/mp/large.bin",
            "s3://test-bucket/mp/large_copy.bin",
            &mp_opts,
        )
        .await
        .unwrap();

    // Parallel range download
    let tmp_dst = tempfile::tempdir().unwrap();
    let dl_path = tmp_dst.path().join("downloaded_large.bin");
    engine
        .cp(
            "s3://test-bucket/mp/large_copy.bin",
            &dl_path.to_string_lossy(),
            &mp_opts,
        )
        .await
        .unwrap();
    assert_eq!(fs::read(&dl_path).unwrap(), large_data);

    // 4. Sync with --delete respecting --exclude (Fixes s5cmd #815 / #852)
    mock_state.objects.insert(
        ("test-bucket".to_string(), "dir/keep_me.secret".to_string()),
        Bytes::from_static(b"protected by exclude"),
    );
    mock_state.objects.insert(
        ("test-bucket".to_string(), "dir/stale.txt".to_string()),
        Bytes::from_static(b"should be deleted"),
    );

    let mut sync_opts = SyncOptions {
        delete: true,
        size_only: true,
        ..Default::default()
    };
    sync_opts.copy_opts.excludes = vec!["*.tmp".to_string(), "*.secret".to_string()];

    engine
        .sync(
            &tmp_src.path().to_string_lossy(),
            "s3://test-bucket/dir/",
            &sync_opts,
        )
        .await
        .unwrap();

    // stale.txt was deleted, keep_me.secret was preserved because it matched --exclude!
    assert!(!mock_state
        .objects
        .contains_key(&("test-bucket".to_string(), "dir/stale.txt".to_string())));
    assert!(mock_state
        .objects
        .contains_key(&("test-bucket".to_string(), "dir/keep_me.secret".to_string())));

    // 5. Security: Path Traversal Protection (Fixes s5cmd CVE / #872)
    let res = engine
        .download_single_object(
            "test-bucket",
            "../../etc/passwd",
            10,
            None,
            &tmp_dst.path().join("evil"),
            &CopyOptions::default(),
            "cp",
        )
        .await;
    assert!(res.is_err());

    // 6. ls, du, head, presign, rb --force
    engine
        .ls(Some("s3://test-bucket/*"), &LsOptions::default())
        .await
        .unwrap();
    engine
        .du("s3://test-bucket/*", &DuOptions::default())
        .await
        .unwrap();
    engine
        .head("s3://test-bucket/dir/hello.txt", None)
        .await
        .unwrap();
    let presigned = engine
        .presign("s3://test-bucket/dir/hello.txt", 900)
        .unwrap();
    assert!(presigned.contains("X-Amz-Signature="));

    engine.rb("s3://test-bucket", true).await.unwrap();
}
