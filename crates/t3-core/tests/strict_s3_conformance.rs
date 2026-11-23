use axum::body::Body;
use axum::extract::{OriginalUri, Path as AxumPath, Query, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::Response;
use axum::Router;
use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use md5::{Digest as Md5Digest, Md5};
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use t3_core::{
    classify_error_exit_code, parse_duration_secs, CopyOptions, RmOptions, SyncOptions, T3Engine,
    T3ExitCode, TarCompression,
};
use t3_s3_client::{AwsCredentials, ResolvedConfig, S3Client, SigV4Signer};
use t3_storage::{compute_adaptive_part_size, ResumeTracker};
use t3_telemetry::{LogLevel, OutputContext};

#[derive(Clone)]
struct StoredObject {
    data: Bytes,
    etag: String,
    last_modified: DateTime<Utc>,
    metadata: HashMap<String, String>,
}

#[derive(Clone)]
struct StrictS3State {
    creds: AwsCredentials,
    region: String,
    signer: Arc<SigV4Signer>,
    buckets: Arc<DashMap<String, String>>,
    objects: Arc<DashMap<(String, String), StoredObject>>,
    upload_meta: Arc<DashMap<String, HashMap<String, String>>>,
    range_get_requests: Arc<AtomicUsize>,
}

impl StrictS3State {
    fn new() -> Self {
        Self {
            creds: AwsCredentials {
                access_key_id: "AKIA_STRICT_CONFORMANCE".to_string(),
                secret_access_key: "SECRET_STRICT_CONFORMANCE_KEY_12345".to_string(),
                session_token: None,
            },
            region: "us-east-1".to_string(),
            signer: Arc::new(SigV4Signer::new()),
            buckets: Arc::new(DashMap::new()),
            objects: Arc::new(DashMap::new()),
            upload_meta: Arc::new(DashMap::new()),
            range_get_requests: Arc::new(AtomicUsize::new(0)),
        }
    }
}

fn s3_xml_error(status: StatusCode, code: &str, message: &str) -> Response {
    let xml = format!("<Error><Code>{code}</Code><Message>{message}</Message></Error>");
    Response::builder()
        .status(status)
        .header("content-type", "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

fn compute_md5_hex(data: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

fn extract_user_metadata(headers: &HeaderMap) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (k, v) in headers.iter() {
        if let Some(stripped) = k.as_str().strip_prefix("x-amz-meta-") {
            if let Ok(val) = v.to_str() {
                map.insert(stripped.to_string(), val.to_string());
            }
        }
    }
    map
}

fn validate_strict_s3_request(
    state: &StrictS3State,
    method: &Method,
    uri: &OriginalUri,
    headers: &HeaderMap,
    body: &[u8],
    require_cl: bool,
) -> Option<Response> {
    // 1. Strict Content-Length validation on all PUT and POST requests
    if require_cl || method == Method::PUT || method == Method::POST {
        let Some(cl_str) = headers.get("content-length").and_then(|v| v.to_str().ok()) else {
            return Some(s3_xml_error(
                StatusCode::LENGTH_REQUIRED,
                "MissingContentLength",
                "You must provide the Content-Length HTTP header.",
            ));
        };
        if cl_str.parse::<usize>().ok() != Some(body.len()) {
            return Some(s3_xml_error(
                StatusCode::BAD_REQUEST,
                "IncompleteBody",
                "Content-Length did not match actual body bytes.",
            ));
        }
    }

    // 2. Strict SigV4 signature verification
    if let Err(reason) = state.signer.verify_request_signature(
        method.as_str(),
        uri.0.path(),
        uri.0.query(),
        headers,
        body,
        &state.creds,
        &state.region,
    ) {
        return Some(s3_xml_error(
            StatusCode::FORBIDDEN,
            "SignatureDoesNotMatch",
            &reason,
        ));
    }

    None
}

fn check_conditional_headers(
    headers: &HeaderMap,
    existing: Option<&StoredObject>,
) -> Option<Response> {
    if let Some(inm) = headers.get("if-none-match").and_then(|v| v.to_str().ok()) {
        let trimmed = inm.trim();
        if trimmed == "*" && existing.is_some() {
            return Some(s3_xml_error(
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                "At least one of the pre-conditions you specified did not hold (If-None-Match: *)",
            ));
        }
        let clean = trimmed.trim_matches('"');
        if existing.is_some_and(|obj| obj.etag == clean) {
            return Some(s3_xml_error(
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                "If-None-Match ETag matched existing object",
            ));
        }
    }

    if let Some(im) = headers.get("if-match").and_then(|v| v.to_str().ok()) {
        let clean = im.trim().trim_matches('"');
        match existing {
            None => {
                return Some(s3_xml_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchKey",
                    "The specified key does not exist for If-Match",
                ));
            }
            Some(obj) if clean != "*" && obj.etag != clean => {
                return Some(s3_xml_error(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "If-Match ETag did not match current object ETag",
                ));
            }
            _ => {}
        }
    }

    None
}

async fn spawn_strict_s3() -> (String, StrictS3State, tokio::task::JoinHandle<()>) {
    let state = StrictS3State::new();
    let app = Router::new()
        .route(
            "/{bucket}",
            axum::routing::put(strict_bucket_put)
                .delete(strict_bucket_delete)
                .head(strict_bucket_head)
                .post(strict_bucket_post)
                .get(strict_bucket_get),
        )
        .route(
            "/{bucket}/{*key}",
            axum::routing::put(strict_object_put)
                .get(strict_object_get)
                .head(strict_object_head)
                .post(strict_object_post)
                .delete(strict_object_delete),
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

async fn strict_bucket_put(
    State(state): State<StrictS3State>,
    method: Method,
    uri: OriginalUri,
    AxumPath(bucket): AxumPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(err) = validate_strict_s3_request(&state, &method, &uri, &headers, &body, true) {
        return err;
    }
    state.buckets.insert(bucket, "Unversioned".to_string());
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .unwrap()
}

async fn strict_bucket_delete(
    State(state): State<StrictS3State>,
    method: Method,
    uri: OriginalUri,
    AxumPath(bucket): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = validate_strict_s3_request(&state, &method, &uri, &headers, &[], false) {
        return err;
    }
    state.buckets.remove(&bucket);
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap()
}

async fn strict_bucket_head(
    State(state): State<StrictS3State>,
    method: Method,
    uri: OriginalUri,
    AxumPath(bucket): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = validate_strict_s3_request(&state, &method, &uri, &headers, &[], false) {
        return err;
    }
    if state.buckets.contains_key(&bucket) {
        Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()
    }
}

async fn strict_bucket_post(
    State(state): State<StrictS3State>,
    method: Method,
    uri: OriginalUri,
    AxumPath(bucket): AxumPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(err) = validate_strict_s3_request(&state, &method, &uri, &headers, &body, true) {
        return err;
    }

    // Strict Content-MD5 verification on DeleteObjects (POST ?delete)
    let Some(md5_hdr) = headers.get("content-md5").and_then(|v| v.to_str().ok()) else {
        return s3_xml_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "Missing required header for this request: Content-MD5",
        );
    };
    let mut md5_hasher = Md5::new();
    md5_hasher.update(&body);
    let expected_md5 = base64::engine::general_purpose::STANDARD.encode(md5_hasher.finalize());
    if md5_hdr != expected_md5 {
        return s3_xml_error(
            StatusCode::BAD_REQUEST,
            "BadDigest",
            "The Content-MD5 you specified did not match what we received.",
        );
    }

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

async fn strict_bucket_get(
    State(state): State<StrictS3State>,
    method: Method,
    uri: OriginalUri,
    AxumPath(bucket): AxumPath<String>,
    Query(q): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = validate_strict_s3_request(&state, &method, &uri, &headers, &[], false) {
        return err;
    }
    if !state.buckets.contains_key(&bucket) {
        return s3_xml_error(
            StatusCode::NOT_FOUND,
            "NoSuchBucket",
            "The specified bucket does not exist",
        );
    }
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let mut items: Vec<(String, StoredObject)> = state
        .objects
        .iter()
        .filter_map(|e| {
            let (b, k) = e.key();
            if b == &bucket && k.starts_with(&prefix) && !k.contains(".part.") {
                Some((k.clone(), e.value().clone()))
            } else {
                None
            }
        })
        .collect();
    items.sort_by(|a, b| a.0.cmp(&b.0));

    let mut xml_out =
        format!("<ListBucketResult><Name>{bucket}</Name><IsTruncated>false</IsTruncated>");
    for (k, obj) in items {
        xml_out.push_str(&format!(
            "<Contents><Key>{k}</Key><LastModified>{}</LastModified><ETag>\"{}\"</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            obj.last_modified.format("%Y-%m-%dT%H:%M:%SZ"),
            obj.etag,
            obj.data.len()
        ));
    }
    xml_out.push_str("</ListBucketResult>");
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(xml_out))
        .unwrap()
}

async fn strict_object_put(
    State(state): State<StrictS3State>,
    method: Method,
    uri: OriginalUri,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(err) = validate_strict_s3_request(&state, &method, &uri, &headers, &body, true) {
        return err;
    }

    if let (Some(uid), Some(pnum)) = (q.get("uploadId"), q.get("partNumber")) {
        let etag = compute_md5_hex(&body);
        state.objects.insert(
            (bucket, format!("{key}.part.{uid}.{pnum}")),
            StoredObject {
                data: body,
                etag: etag.clone(),
                last_modified: Utc::now(),
                metadata: HashMap::new(),
            },
        );
        return Response::builder()
            .status(StatusCode::OK)
            .header("etag", format!("\"{etag}\""))
            .body(Body::empty())
            .unwrap();
    }

    let existing = state
        .objects
        .get(&(bucket.clone(), key.clone()))
        .map(|r| r.value().clone());
    if let Some(err) = check_conditional_headers(&headers, existing.as_ref()) {
        return err;
    }

    if let Some(copy_src) = headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
    {
        let trimmed = copy_src.trim_start_matches('/');
        let mut sp = trimmed.splitn(2, '/');
        let sb = sp.next().unwrap_or("");
        let sk = sp.next().unwrap_or("");
        let Some(src_obj) = state
            .objects
            .get(&(sb.to_string(), sk.to_string()))
            .map(|r| r.value().clone())
        else {
            return s3_xml_error(
                StatusCode::NOT_FOUND,
                "NoSuchKey",
                "The specified source key does not exist",
            );
        };

        let directive = headers
            .get("x-amz-metadata-directive")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("COPY");
        let meta = if directive.eq_ignore_ascii_case("REPLACE") {
            extract_user_metadata(&headers)
        } else {
            src_obj.metadata.clone()
        };
        let etag = src_obj.etag.clone();
        state.objects.insert(
            (bucket, key),
            StoredObject {
                data: src_obj.data,
                etag: etag.clone(),
                last_modified: Utc::now(),
                metadata: meta,
            },
        );
        return Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(format!(
                "<CopyObjectResult><LastModified>2026-11-22T12:00:00Z</LastModified><ETag>\"{etag}\"</ETag></CopyObjectResult>"
            )))
            .unwrap();
    }

    let etag = compute_md5_hex(&body);
    let metadata = extract_user_metadata(&headers);
    state.objects.insert(
        (bucket, key),
        StoredObject {
            data: body,
            etag: etag.clone(),
            last_modified: Utc::now(),
            metadata,
        },
    );
    Response::builder()
        .status(StatusCode::OK)
        .header("etag", format!("\"{etag}\""))
        .body(Body::empty())
        .unwrap()
}

async fn strict_object_post(
    State(state): State<StrictS3State>,
    method: Method,
    uri: OriginalUri,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(err) = validate_strict_s3_request(&state, &method, &uri, &headers, &body, true) {
        return err;
    }

    if q.contains_key("uploads") {
        let uid = format!(
            "uid-{}",
            state.range_get_requests.fetch_add(1, Ordering::SeqCst) + 1000
        );
        state
            .upload_meta
            .insert(uid.clone(), extract_user_metadata(&headers));
        let xml_out = format!(
            "<InitiateMultipartUploadResult><Bucket>{bucket}</Bucket><Key>{key}</Key><UploadId>{uid}</UploadId></InitiateMultipartUploadResult>"
        );
        return Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(xml_out))
            .unwrap();
    }

    if let Some(uid) = q.get("uploadId") {
        let existing = state
            .objects
            .get(&(bucket.clone(), key.clone()))
            .map(|r| r.value().clone());
        if let Some(err) = check_conditional_headers(&headers, existing.as_ref()) {
            return err;
        }

        let mut combined = Vec::new();
        let mut p = 1;
        while let Some((_, part_obj)) = state
            .objects
            .remove(&(bucket.clone(), format!("{key}.part.{uid}.{p}")))
        {
            combined.extend_from_slice(&part_obj.data);
            p += 1;
        }
        let etag = format!("{}-{}", compute_md5_hex(&combined), p - 1);
        let metadata = state
            .upload_meta
            .remove(uid)
            .map(|(_, m)| m)
            .unwrap_or_default();
        state.objects.insert(
            (bucket.clone(), key.clone()),
            StoredObject {
                data: Bytes::from(combined),
                etag: etag.clone(),
                last_modified: Utc::now(),
                metadata,
            },
        );
        let xml_out = format!(
            "<CompleteMultipartUploadResult><Bucket>{bucket}</Bucket><Key>{key}</Key><ETag>\"{etag}\"</ETag></CompleteMultipartUploadResult>"
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

async fn strict_object_head(
    State(state): State<StrictS3State>,
    method: Method,
    uri: OriginalUri,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = validate_strict_s3_request(&state, &method, &uri, &headers, &[], false) {
        return err;
    }
    if let Some(obj) = state.objects.get(&(bucket, key)) {
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header("content-length", obj.data.len().to_string())
            .header("etag", format!("\"{}\"", obj.etag))
            .header(
                "last-modified",
                obj.last_modified
                    .format("%a, %d %b %Y %H:%M:%S GMT")
                    .to_string(),
            );
        for (mk, mv) in &obj.metadata {
            builder = builder.header(format!("x-amz-meta-{mk}"), mv);
        }
        builder.body(Body::empty()).unwrap()
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()
    }
}

async fn strict_object_get(
    State(state): State<StrictS3State>,
    method: Method,
    uri: OriginalUri,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = validate_strict_s3_request(&state, &method, &uri, &headers, &[], false) {
        return err;
    }
    let Some(obj) = state.objects.get(&(bucket, key)).map(|r| r.value().clone()) else {
        return s3_xml_error(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "The specified key does not exist.",
        );
    };

    if let Some(range_str) = headers.get("range").and_then(|v| v.to_str().ok()) {
        state.range_get_requests.fetch_add(1, Ordering::SeqCst);
        if let Some(spec) = range_str.strip_prefix("bytes=") {
            if let Some((s, e)) = spec.split_once('-') {
                if let (Ok(start), Ok(end)) = (s.parse::<usize>(), e.parse::<usize>()) {
                    let total = obj.data.len();
                    let end_clamped = end.min(total.saturating_sub(1));
                    let slice = obj.data.slice(start..=end_clamped);
                    return Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header("content-length", slice.len().to_string())
                        .header(
                            "content-range",
                            format!("bytes {start}-{end_clamped}/{total}"),
                        )
                        .header("etag", format!("\"{}\"", obj.etag))
                        .body(Body::from(slice))
                        .unwrap();
                }
            }
        }
    }

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("content-length", obj.data.len().to_string())
        .header("etag", format!("\"{}\"", obj.etag));
    for (mk, mv) in &obj.metadata {
        builder = builder.header(format!("x-amz-meta-{mk}"), mv);
    }
    builder.body(Body::from(obj.data)).unwrap()
}

async fn strict_object_delete(
    State(state): State<StrictS3State>,
    method: Method,
    uri: OriginalUri,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Some(err) = validate_strict_s3_request(&state, &method, &uri, &headers, &[], false) {
        return err;
    }
    state.objects.remove(&(bucket, key));
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap()
}

fn build_strict_engine(endpoint: &str, state: &StrictS3State, unsigned_payload: bool) -> T3Engine {
    let cfg = ResolvedConfig {
        credentials: Some(state.creds.clone()),
        region: state.region.clone(),
        endpoints: vec![endpoint.to_string()],
        no_sign_request: false,
        no_verify_ssl: false,
        use_list_objects_v1: false,
        request_payer: None,
        retry_count: 3,
        unsigned_payload,
    };
    let output = OutputContext::new(false, false, false, false, LogLevel::Error).with_quiet(true);
    let client = S3Client::new(cfg, Some(output.stats.clone())).unwrap();
    T3Engine::new(client, output, 32)
}

#[tokio::test]
async fn test_strict_s3_conformance_all_v0_2_0_features() {
    let (endpoint, state, _handle) = spawn_strict_s3().await;
    // Test with full SHA-256 payload signing (unsigned_payload = false) against strict SigV4 & Content-Length server!
    let engine = build_strict_engine(&endpoint, &state, false);

    // 1. CreateBucket (0-byte PUT with strict Content-Length: 0 and SigV4)
    engine.mb("s3://cache-bucket").await.unwrap();

    // 2. t3 exists & stable exit codes (Item 4)
    assert!(engine.exists("s3://cache-bucket", None).await.unwrap());
    let missing_err = engine
        .exists("s3://cache-bucket/_pointer", None)
        .await
        .unwrap_err();
    assert_eq!(classify_error_exit_code(&missing_err), T3ExitCode::NotFound);
    // Hard 404 must NEVER retry (Item 8)
    assert_eq!(engine.output.stats.snapshot().retries, 0);

    // 3. Conditional (CAS) writes for _pointer & metadata stamping (Items 6 & 7)
    let tmp = tempfile::tempdir().unwrap();
    let ptr_file = tmp.path().join("_pointer");
    fs::write(&ptr_file, b"gen-001").unwrap();

    let mut cas_create_opts = CopyOptions::default();
    cas_create_opts.put_opts.if_none_match = Some("*".to_string());
    cas_create_opts
        .put_opts
        .metadata
        .insert("gh-repo".to_string(), "ehlerst/t3".to_string());
    cas_create_opts
        .put_opts
        .metadata
        .insert("generation".to_string(), "1".to_string());

    // First writer wins with If-None-Match: *
    engine
        .cp(
            &ptr_file.to_string_lossy(),
            "s3://cache-bucket/_pointer",
            &cas_create_opts,
        )
        .await
        .unwrap();

    // Verify metadata was stamped on _pointer
    let head1 = engine
        .s3
        .head_object("cache-bucket", "_pointer", None)
        .await
        .unwrap();
    assert_eq!(
        head1.metadata.get("gh-repo").map(String::as_str),
        Some("ehlerst/t3")
    );
    assert_eq!(
        head1.metadata.get("generation").map(String::as_str),
        Some("1")
    );
    let etag1 = head1.etag.unwrap();

    // Second concurrent writer with If-None-Match: * fails with HTTP 412 -> T3ExitCode::PreconditionFailed (4) and 0 retries!
    let retries_before = engine.output.stats.snapshot().retries;
    let conflict_err = engine
        .cp(
            &ptr_file.to_string_lossy(),
            "s3://cache-bucket/_pointer",
            &cas_create_opts,
        )
        .await
        .unwrap_err();
    assert_eq!(
        classify_error_exit_code(&conflict_err),
        T3ExitCode::PreconditionFailed
    );
    assert_eq!(engine.output.stats.snapshot().retries, retries_before);

    // Compare-And-Swap update with If-Match: <etag1> succeeds!
    fs::write(&ptr_file, b"gen-002").unwrap();
    let mut cas_update_opts = CopyOptions::default();
    cas_update_opts.put_opts.if_match = Some(etag1.clone());
    cas_update_opts
        .put_opts
        .metadata
        .insert("generation".to_string(), "2".to_string());
    engine
        .cp(
            &ptr_file.to_string_lossy(),
            "s3://cache-bucket/_pointer",
            &cas_update_opts,
        )
        .await
        .unwrap();

    // Compare-And-Swap with stale etag1 now fails with PreconditionFailed (4)!
    let stale_err = engine
        .cp(
            &ptr_file.to_string_lossy(),
            "s3://cache-bucket/_pointer",
            &cas_update_opts,
        )
        .await
        .unwrap_err();
    assert_eq!(
        classify_error_exit_code(&stale_err),
        T3ExitCode::PreconditionFailed
    );

    // 4. Single-Object Tar-Stream Mode (`tar-sync` & `cp --tar-stream`) across lz4, zstd, gzip, none (Item 1)
    let cache_src = tmp.path().join("buildx-cache");
    fs::create_dir_all(cache_src.join("blobs/sha256")).unwrap();
    for i in 0..40 {
        let content = format!(
            "{{\"blob\":{i},\"layer\":\"sha256:{:064x}\",\"payload\":\"{}\"}}",
            i * 997,
            "cache-data-segment-".repeat(50)
        );
        fs::write(
            cache_src.join(format!("blobs/sha256/blob_{i:03}.json")),
            content.as_bytes(),
        )
        .unwrap();
    }

    for (codec, ext) in [
        (TarCompression::Lz4, "tar.lz4"),
        (TarCompression::Zstd, "tar.zst"),
        (TarCompression::Gzip, "tar.gz"),
        (TarCompression::None, "tar"),
    ] {
        let s3_uri = format!("s3://cache-bucket/archives/cache.{ext}");
        let mut tar_opts = CopyOptions {
            tar_stream: true,
            compression: codec,
            ..Default::default()
        };
        tar_opts
            .put_opts
            .metadata
            .insert("codec".to_string(), ext.to_string());

        // Upload directory as single compressed tar object
        engine
            .cp(&cache_src.to_string_lossy(), &s3_uri, &tar_opts)
            .await
            .unwrap();

        // Restore using `Auto` magic-byte detection
        let restore_dir = tmp.path().join(format!("restored-{ext}"));
        let restore_opts = CopyOptions {
            tar_stream: true,
            compression: TarCompression::Auto,
            ..Default::default()
        };
        engine
            .cp(&s3_uri, &restore_dir.to_string_lossy(), &restore_opts)
            .await
            .unwrap();

        for i in 0..40 {
            let orig = fs::read(cache_src.join(format!("blobs/sha256/blob_{i:03}.json"))).unwrap();
            let rest =
                fs::read(restore_dir.join(format!("blobs/sha256/blob_{i:03}.json"))).unwrap();
            assert_eq!(orig, rest, "Mismatch after tar-stream restore for {ext}");
        }
    }

    // 5. Adaptive Part Sizing & Resumable Multipart Range Download (`--continue` / `.t3part`) (Items 2 & 3)
    assert_eq!(
        compute_adaptive_part_size(3 * 1024 * 1024, None),
        16 * 1024 * 1024
    ); // 3 MiB takes single-PUT fast path
    assert_eq!(
        compute_adaptive_part_size(64 * 1024 * 1024, None),
        16 * 1024 * 1024
    );
    assert_eq!(
        compute_adaptive_part_size(500 * 1024 * 1024, None),
        32 * 1024 * 1024
    );
    assert_eq!(
        compute_adaptive_part_size(5 * 1024 * 1024 * 1024, None),
        64 * 1024 * 1024
    );

    // Upload a 15 MiB object and test resumable range download with 5 MiB parts (3 parts total)
    let big_data: Vec<u8> = (0..(15 * 1024 * 1024)).map(|i| (i % 251) as u8).collect();
    let big_src = tmp.path().join("big_layer.bin");
    fs::write(&big_src, &big_data).unwrap();
    let part_5m = 5 * 1024 * 1024;
    let resume_opts = CopyOptions {
        concurrency: 3,
        part_size_bytes: part_5m,
        explicit_part_size: Some(part_5m),
        resume: true,
        ..Default::default()
    };
    engine
        .cp(
            &big_src.to_string_lossy(),
            "s3://cache-bucket/layers/big_layer.bin",
            &resume_opts,
        )
        .await
        .unwrap();

    // Pre-populate `.t3part` and `.t3part.meta` with parts 0 and 1 already completed (10 MiB of 15 MiB)
    let big_dst = tmp.path().join("resumed_big_layer.bin");
    let (tracker, _) =
        ResumeTracker::load_or_init(&big_dst, None, big_data.len() as u64, part_5m, false);
    fs::write(&tracker.part_file_path, &big_data).unwrap();
    tracker.mark_part_completed(0).unwrap();
    tracker.mark_part_completed(1).unwrap();

    state.range_get_requests.store(0, Ordering::SeqCst);
    let resumed_before = engine.output.stats.snapshot().resumed_bytes;
    engine
        .cp(
            "s3://cache-bucket/layers/big_layer.bin",
            &big_dst.to_string_lossy(),
            &resume_opts,
        )
        .await
        .unwrap();

    // Only 1 range GET request (for part 2) should have been sent!
    assert_eq!(state.range_get_requests.load(Ordering::SeqCst), 1);
    assert_eq!(
        engine.output.stats.snapshot().resumed_bytes - resumed_before,
        10 * 1024 * 1024
    );
    assert_eq!(fs::read(&big_dst).unwrap(), big_data);
    assert!(!tracker.part_file_path.exists());
    assert!(!tracker.meta_file_path.exists());

    // 6. Age-scoped pruning: `rm --older-than` and `sync --delete --exclude-newer-than` (Item 9)
    assert_eq!(parse_duration_secs("2d12h").unwrap(), 216_000);
    state.objects.insert(
        (
            "cache-bucket".to_string(),
            "staging/old_run.bin".to_string(),
        ),
        StoredObject {
            data: Bytes::from_static(b"old"),
            etag: "e1".to_string(),
            last_modified: Utc::now() - ChronoDuration::hours(5),
            metadata: HashMap::new(),
        },
    );
    state.objects.insert(
        (
            "cache-bucket".to_string(),
            "staging/new_run.bin".to_string(),
        ),
        StoredObject {
            data: Bytes::from_static(b"new"),
            etag: "e2".to_string(),
            last_modified: Utc::now() - ChronoDuration::minutes(5),
            metadata: HashMap::new(),
        },
    );

    let rm_opts = RmOptions {
        older_than_secs: Some(3600), // 1h
        ..Default::default()
    };
    engine
        .rm(&["s3://cache-bucket/staging/*".to_string()], &rm_opts)
        .await
        .unwrap();

    // old_run.bin (5h old) was pruned; new_run.bin (5m old) was protected!
    assert!(!state.objects.contains_key(&(
        "cache-bucket".to_string(),
        "staging/old_run.bin".to_string()
    )));
    assert!(state.objects.contains_key(&(
        "cache-bucket".to_string(),
        "staging/new_run.bin".to_string()
    )));

    // Now test `sync --delete --exclude-newer-than 1h`
    state.objects.insert(
        (
            "cache-bucket".to_string(),
            "staging/stale_old.bin".to_string(),
        ),
        StoredObject {
            data: Bytes::from_static(b"stale-old"),
            etag: "e3".to_string(),
            last_modified: Utc::now() - ChronoDuration::hours(4),
            metadata: HashMap::new(),
        },
    );
    let empty_local = tmp.path().join("empty_sync_src");
    fs::create_dir_all(&empty_local).unwrap();
    let sync_opts = SyncOptions {
        delete: true,
        size_only: true,
        exclude_newer_than_secs: Some(3600),
        ..Default::default()
    };
    engine
        .sync(
            &empty_local.to_string_lossy(),
            "s3://cache-bucket/staging/",
            &sync_opts,
        )
        .await
        .unwrap();

    // stale_old.bin (4h old) was deleted by sync --delete; new_run.bin (5m old) was preserved by --exclude-newer-than 1h!
    assert!(!state.objects.contains_key(&(
        "cache-bucket".to_string(),
        "staging/stale_old.bin".to_string()
    )));
    assert!(state.objects.contains_key(&(
        "cache-bucket".to_string(),
        "staging/new_run.bin".to_string()
    )));
}
