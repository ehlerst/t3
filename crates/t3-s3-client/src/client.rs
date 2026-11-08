use crate::auth::ResolvedConfig;
use crate::sigv4::{aws_uri_encode, sha256_hex, SigV4Signer};
use crate::types::{
    BucketInfo, CompletedPartInfo, DeleteObjectsOutput, ListObjectsPage, ObjectHead,
    PutObjectOptions, S3ClientError,
};
use crate::xml;
use base64::Engine;
use bytes::Bytes;
use chrono::Utc;
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use md5::{Digest as Md5Digest, Md5};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use t3_telemetry::TelemetryStats;
use tokio::sync::Semaphore;
use url::Url;

#[derive(Clone)]
pub struct S3Client {
    http: reqwest::Client,
    config: Arc<ResolvedConfig>,
    signer: Arc<SigV4Signer>,
    endpoint_idx: Arc<AtomicUsize>,
    http_sem: Arc<Semaphore>,
    large_sem: Arc<Semaphore>,
    stats: Option<Arc<TelemetryStats>>,
}

impl S3Client {
    pub fn new(
        config: ResolvedConfig,
        stats: Option<Arc<TelemetryStats>>,
    ) -> Result<Self, S3ClientError> {
        let max_inflight = (config.endpoints.len().max(1) * 24).clamp(24, 128);
        let large_inflight = (config.endpoints.len().max(1) * 6).clamp(6, 32);
        let http = reqwest::Client::builder()
            .tcp_nodelay(true)
            .pool_max_idle_per_host(max_inflight)
            .pool_idle_timeout(Duration::from_secs(90))
            .danger_accept_invalid_certs(config.no_verify_ssl)
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| S3ClientError::InvalidConfig(e.to_string()))?;

        Ok(Self {
            http,
            config: Arc::new(config),
            signer: Arc::new(SigV4Signer::new()),
            endpoint_idx: Arc::new(AtomicUsize::new(0)),
            http_sem: Arc::new(Semaphore::new(max_inflight)),
            large_sem: Arc::new(Semaphore::new(large_inflight)),
            stats,
        })
    }

    pub fn config(&self) -> &ResolvedConfig {
        &self.config
    }

    pub fn with_endpoint(
        &self,
        endpoint: &str,
        region: Option<&str>,
    ) -> Result<Self, S3ClientError> {
        let mut cfg = (*self.config).clone();
        cfg.endpoints = vec![endpoint.trim_end_matches('/').to_string()];
        if let Some(r) = region {
            cfg.region = r.to_string();
        }
        Self::new(cfg, self.stats.clone())
    }

    #[inline]
    fn next_endpoint(&self) -> &str {
        let eps = &self.config.endpoints;
        if eps.len() == 1 {
            &eps[0]
        } else {
            let idx = self.endpoint_idx.fetch_add(1, Ordering::Relaxed);
            &eps[idx % eps.len()]
        }
    }

    fn build_url(
        &self,
        bucket: Option<&str>,
        key: Option<&str>,
        query: &[(&str, &str)],
    ) -> Result<Url, S3ClientError> {
        let base = self.next_endpoint();
        let mut url_str = String::with_capacity(base.len() + 64);
        url_str.push_str(base);

        if let Some(b) = bucket {
            url_str.push('/');
            url_str.push_str(&aws_uri_encode(b));
            if let Some(k) = key {
                url_str.push('/');
                for (i, seg) in k.split('/').enumerate() {
                    if i > 0 {
                        url_str.push('/');
                    }
                    url_str.push_str(&aws_uri_encode(seg));
                }
            }
        } else {
            url_str.push('/');
        }

        let mut url =
            Url::parse(&url_str).map_err(|e| S3ClientError::InvalidConfig(e.to_string()))?;
        if !query.is_empty() {
            let mut qp = url.query_pairs_mut();
            for (k, v) in query {
                qp.append_pair(k, v);
            }
        }
        Ok(url)
    }

    async fn execute_raw(
        &self,
        method: Method,
        bucket: Option<&str>,
        key: Option<&str>,
        query: &[(&str, &str)],
        extra_headers: HeaderMap,
        body: Bytes,
    ) -> Result<(u16, HeaderMap, Bytes), S3ClientError> {
        let max_retries = self.config.retry_count;
        let mut attempt = 0u32;

        loop {
            let url = self.build_url(bucket, key, query)?;
            let mut headers = extra_headers.clone();

            if let Some(ref payer) = self.config.request_payer {
                if let Ok(val) = HeaderValue::from_str(payer) {
                    headers.insert("x-amz-request-payer", val);
                }
            }

            if !self.config.no_sign_request {
                if let Some(ref creds) = self.config.credentials {
                    let payload_hash = if self.config.unsigned_payload {
                        "UNSIGNED-PAYLOAD".to_string()
                    } else if body.is_empty() {
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                            .to_string()
                    } else {
                        sha256_hex(&body)
                    };
                    self.signer.sign_request(
                        method.as_str(),
                        &url,
                        &mut headers,
                        &payload_hash,
                        creds,
                        &self.config.region,
                        Utc::now(),
                    );
                }
            }

            let req = self
                .http
                .request(method.clone(), url)
                .headers(headers)
                .body(body.clone());

            let _http_permit = self.http_sem.acquire().await.unwrap();
            match req.send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let resp_headers = resp.headers().clone();
                    let resp_bytes = resp
                        .bytes()
                        .await
                        .map_err(|e| S3ClientError::Transport(e.to_string()))?;

                    if (200..300).contains(&status) {
                        return Ok((status, resp_headers, resp_bytes));
                    }

                    let err = if status == 404 && resp_bytes.is_empty() {
                        S3ClientError::NotFound(format!(
                            "s3://{}/{}",
                            bucket.unwrap_or(""),
                            key.unwrap_or("")
                        ))
                    } else {
                        xml::parse_s3_error(status, &resp_bytes)
                    };

                    if attempt < max_retries && err.is_retryable() {
                        attempt += 1;
                        if let Some(ref st) = self.stats {
                            st.record_retry();
                        }
                        let backoff_ms = (15u64 << attempt.min(7)) + fastrand::u64(0..25);
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    return Err(err);
                }
                Err(e) => {
                    let err = S3ClientError::Transport(e.to_string());
                    if attempt < max_retries {
                        attempt += 1;
                        if let Some(ref st) = self.stats {
                            st.record_retry();
                        }
                        let backoff_ms = (15u64 << attempt.min(7)) + fastrand::u64(0..25);
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>, S3ClientError> {
        let (_, _, body) = self
            .execute_raw(Method::GET, None, None, &[], HeaderMap::new(), Bytes::new())
            .await?;
        xml::parse_list_buckets(&body)
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<(), S3ClientError> {
        let body = if self.config.region != "us-east-1" {
            Bytes::from(format!(
                "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>{}</LocationConstraint></CreateBucketConfiguration>",
                self.config.region
            ))
        } else {
            Bytes::new()
        };
        self.execute_raw(Method::PUT, Some(bucket), None, &[], HeaderMap::new(), body)
            .await?;
        Ok(())
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<(), S3ClientError> {
        self.execute_raw(
            Method::DELETE,
            Some(bucket),
            None,
            &[],
            HeaderMap::new(),
            Bytes::new(),
        )
        .await?;
        Ok(())
    }

    pub async fn head_bucket(&self, bucket: &str) -> Result<Option<String>, S3ClientError> {
        let (_, headers, _) = self
            .execute_raw(
                Method::HEAD,
                Some(bucket),
                None,
                &[],
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        let region = headers
            .get("x-amz-bucket-region")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        Ok(region)
    }

    pub async fn get_bucket_versioning(&self, bucket: &str) -> Result<String, S3ClientError> {
        let (_, _, body) = self
            .execute_raw(
                Method::GET,
                Some(bucket),
                None,
                &[("versioning", "")],
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        Ok(xml::parse_bucket_versioning(&body).unwrap_or_else(|| "Unversioned".to_string()))
    }

    pub async fn put_bucket_versioning(
        &self,
        bucket: &str,
        status: &str,
    ) -> Result<(), S3ClientError> {
        let xml_body = format!(
            "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>{}</Status></VersioningConfiguration>",
            status
        );
        self.execute_raw(
            Method::PUT,
            Some(bucket),
            None,
            &[("versioning", "")],
            HeaderMap::new(),
            Bytes::from(xml_body),
        )
        .await?;
        Ok(())
    }

    pub async fn list_objects_page(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: u32,
    ) -> Result<ListObjectsPage, S3ClientError> {
        let max_keys_str = max_keys.to_string();
        let mut query = Vec::with_capacity(6);
        if self.config.use_list_objects_v1 {
            if let Some(p) = prefix {
                if !p.is_empty() {
                    query.push(("prefix", p));
                }
            }
            if let Some(d) = delimiter {
                if !d.is_empty() {
                    query.push(("delimiter", d));
                }
            }
            if let Some(marker) = continuation_token {
                query.push(("marker", marker));
            }
            query.push(("max-keys", &max_keys_str));
        } else {
            query.push(("list-type", "2"));
            if let Some(p) = prefix {
                if !p.is_empty() {
                    query.push(("prefix", p));
                }
            }
            if let Some(d) = delimiter {
                if !d.is_empty() {
                    query.push(("delimiter", d));
                }
            }
            if let Some(tok) = continuation_token {
                query.push(("continuation-token", tok));
            }
            query.push(("max-keys", &max_keys_str));
        }

        let (_, _, body) = self
            .execute_raw(
                Method::GET,
                Some(bucket),
                None,
                &query,
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        xml::parse_list_objects(&body)
    }

    pub async fn list_object_versions_page(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
    ) -> Result<ListObjectsPage, S3ClientError> {
        let mut query = vec![("versions", "")];
        if let Some(p) = prefix {
            if !p.is_empty() {
                query.push(("prefix", p));
            }
        }
        if let Some(km) = key_marker {
            query.push(("key-marker", km));
        }
        if let Some(vm) = version_id_marker {
            query.push(("version-id-marker", vm));
        }
        let (_, _, body) = self
            .execute_raw(
                Method::GET,
                Some(bucket),
                None,
                &query,
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;
        xml::parse_list_object_versions(&body)
    }

    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<ObjectHead, S3ClientError> {
        let mut query = Vec::new();
        if let Some(vid) = version_id {
            query.push(("versionId", vid));
        }
        let (_, headers, _) = self
            .execute_raw(
                Method::HEAD,
                Some(bucket),
                Some(key),
                &query,
                HeaderMap::new(),
                Bytes::new(),
            )
            .await?;

        Ok(extract_object_head(&headers))
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
        version_id: Option<&str>,
    ) -> Result<(ObjectHead, Bytes), S3ClientError> {
        let mut query = Vec::new();
        if let Some(vid) = version_id {
            query.push(("versionId", vid));
        }
        let mut headers = HeaderMap::new();
        if let Some((start, end)) = range {
            let range_val = format!("bytes={start}-{end}");
            headers.insert("range", HeaderValue::from_str(&range_val).unwrap());
        }

        let (_, resp_headers, body) = self
            .execute_raw(
                Method::GET,
                Some(bucket),
                Some(key),
                &query,
                headers,
                Bytes::new(),
            )
            .await?;

        Ok((extract_object_head(&resp_headers), body))
    }

    pub async fn get_object_bytes(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
        version_id: Option<&str>,
    ) -> Result<Bytes, S3ClientError> {
        let mut query = Vec::new();
        if let Some(vid) = version_id {
            query.push(("versionId", vid));
        }
        let mut headers = HeaderMap::new();
        if let Some((start, end)) = range {
            let range_val = format!("bytes={start}-{end}");
            headers.insert("range", HeaderValue::from_str(&range_val).unwrap());
        }

        let (_, _, body) = self
            .execute_raw(
                Method::GET,
                Some(bucket),
                Some(key),
                &query,
                headers,
                Bytes::new(),
            )
            .await?;

        Ok(body)
    }

    pub async fn download_object_to_file(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        dst_file: &std::path::Path,
        _size_hint: u64,
    ) -> Result<u64, S3ClientError> {
        let mut query = Vec::new();
        if let Some(vid) = version_id {
            query.push(("versionId", vid));
        }

        let max_retries = self.config.retry_count;
        let mut attempt = 0u32;

        loop {
            let url = self.build_url(Some(bucket), Some(key), &query)?;
            let mut headers = HeaderMap::new();

            if let Some(ref payer) = self.config.request_payer {
                if let Ok(val) = HeaderValue::from_str(payer) {
                    headers.insert("x-amz-request-payer", val);
                }
            }

            if !self.config.no_sign_request {
                if let Some(ref creds) = self.config.credentials {
                    let payload_hash = if self.config.unsigned_payload {
                        "UNSIGNED-PAYLOAD"
                    } else {
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                    };
                    self.signer.sign_request(
                        "GET",
                        &url,
                        &mut headers,
                        payload_hash,
                        creds,
                        &self.config.region,
                        Utc::now(),
                    );
                }
            }

            let req = self.http.request(Method::GET, url).headers(headers);
            let sem = if _size_hint >= 1024 * 1024 {
                &self.large_sem
            } else {
                &self.http_sem
            };
            let _http_permit = sem.acquire().await.unwrap();

            match req.send().await {
                Ok(mut resp) => {
                    let status = resp.status().as_u16();
                    if (200..300).contains(&status) {
                        use std::io::Write;
                        let mut file = std::fs::File::create(dst_file)
                            .map_err(|e| S3ClientError::Transport(e.to_string()))?;
                        let mut total = 0u64;
                        while let Some(chunk) = resp
                            .chunk()
                            .await
                            .map_err(|e| S3ClientError::Transport(e.to_string()))?
                        {
                            file.write_all(&chunk)
                                .map_err(|e| S3ClientError::Transport(e.to_string()))?;
                            total += chunk.len() as u64;
                        }
                        drop(_http_permit);
                        return Ok(total);
                    }

                    let resp_bytes = resp
                        .bytes()
                        .await
                        .map_err(|e| S3ClientError::Transport(e.to_string()))?;
                    let err = if status == 404 && resp_bytes.is_empty() {
                        S3ClientError::NotFound(format!("s3://{bucket}/{key}"))
                    } else {
                        xml::parse_s3_error(status, &resp_bytes)
                    };

                    if attempt < max_retries && err.is_retryable() {
                        attempt += 1;
                        if let Some(ref st) = self.stats {
                            st.record_retry();
                        }
                        let backoff_ms = (15u64 << attempt.min(7)) + fastrand::u64(0..25);
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    return Err(err);
                }
                Err(e) => {
                    let err = S3ClientError::Transport(e.to_string());
                    if attempt < max_retries {
                        attempt += 1;
                        if let Some(ref st) = self.stats {
                            st.record_retry();
                        }
                        let backoff_ms = (15u64 << attempt.min(7)) + fastrand::u64(0..25);
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        opts: &PutObjectOptions,
    ) -> Result<String, S3ClientError> {
        let mut headers = build_put_headers(opts);
        if opts.checksum_crc32c {
            let crc = crc32fast::hash(&body).to_be_bytes();
            let b64 = base64::engine::general_purpose::STANDARD.encode(crc);
            if let Ok(val) = HeaderValue::from_str(&b64) {
                headers.insert("x-amz-checksum-crc32", val);
            }
        }

        let (_, resp_headers, _) = self
            .execute_raw(Method::PUT, Some(bucket), Some(key), &[], headers, body)
            .await?;

        let etag = resp_headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string())
            .unwrap_or_default();
        Ok(etag)
    }

    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        src_version_id: Option<&str>,
        dst_bucket: &str,
        dst_key: &str,
        opts: &PutObjectOptions,
    ) -> Result<String, S3ClientError> {
        let mut headers = build_put_headers(opts);
        let encoded_src = format!(
            "/{}/{}",
            aws_uri_encode(src_bucket),
            src_key
                .split('/')
                .map(aws_uri_encode)
                .collect::<Vec<_>>()
                .join("/")
        );
        let copy_src = match src_version_id {
            Some(vid) => format!("{encoded_src}?versionId={}", aws_uri_encode(vid)),
            None => encoded_src,
        };
        headers.insert(
            "x-amz-copy-source",
            HeaderValue::from_str(&copy_src)
                .map_err(|e| S3ClientError::InvalidConfig(e.to_string()))?,
        );
        if let Some(ref dir) = opts.metadata_directive {
            if let Ok(val) = HeaderValue::from_str(dir) {
                headers.insert("x-amz-metadata-directive", val);
            }
        }

        let (_, _, body) = self
            .execute_raw(
                Method::PUT,
                Some(dst_bucket),
                Some(dst_key),
                &[],
                headers,
                Bytes::new(),
            )
            .await?;

        Ok(xml::parse_etag_from_xml(&body).unwrap_or_default())
    }

    pub async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        opts: &PutObjectOptions,
    ) -> Result<String, S3ClientError> {
        let headers = build_put_headers(opts);
        let (_, _, body) = self
            .execute_raw(
                Method::POST,
                Some(bucket),
                Some(key),
                &[("uploads", "")],
                headers,
                Bytes::new(),
            )
            .await?;
        xml::parse_initiate_multipart_upload(&body)
    }

    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: Bytes,
    ) -> Result<String, S3ClientError> {
        let part_str = part_number.to_string();
        let (_, resp_headers, _) = self
            .execute_raw(
                Method::PUT,
                Some(bucket),
                Some(key),
                &[("partNumber", &part_str), ("uploadId", upload_id)],
                HeaderMap::new(),
                body,
            )
            .await?;

        let etag = resp_headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim_matches('"').to_string())
            .unwrap_or_default();
        Ok(etag)
    }

    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        mut parts: Vec<CompletedPartInfo>,
    ) -> Result<String, S3ClientError> {
        parts.sort_by_key(|p| p.part_number);
        let xml_body = xml::serialize_complete_multipart_upload(&parts);
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/xml"));
        let (_, _, resp_body) = self
            .execute_raw(
                Method::POST,
                Some(bucket),
                Some(key),
                &[("uploadId", upload_id)],
                headers,
                Bytes::from(xml_body),
            )
            .await?;
        Ok(xml::parse_etag_from_xml(&resp_body).unwrap_or_default())
    }

    pub async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), S3ClientError> {
        self.execute_raw(
            Method::DELETE,
            Some(bucket),
            Some(key),
            &[("uploadId", upload_id)],
            HeaderMap::new(),
            Bytes::new(),
        )
        .await?;
        Ok(())
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<(), S3ClientError> {
        let mut query = Vec::new();
        if let Some(vid) = version_id {
            query.push(("versionId", vid));
        }
        self.execute_raw(
            Method::DELETE,
            Some(bucket),
            Some(key),
            &query,
            HeaderMap::new(),
            Bytes::new(),
        )
        .await?;
        Ok(())
    }

    pub async fn delete_objects(
        &self,
        bucket: &str,
        keys: &[(String, Option<String>)],
    ) -> Result<DeleteObjectsOutput, S3ClientError> {
        if keys.is_empty() {
            return Ok(DeleteObjectsOutput {
                deleted: Vec::new(),
                errors: Vec::new(),
            });
        }
        let xml_body = xml::serialize_delete_objects(keys, false);
        let body_bytes = Bytes::from(xml_body);

        let mut md5_hasher = Md5::new();
        md5_hasher.update(&body_bytes);
        let content_md5 = base64::engine::general_purpose::STANDARD.encode(md5_hasher.finalize());

        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/xml"));
        if let Ok(val) = HeaderValue::from_str(&content_md5) {
            headers.insert("content-md5", val);
        }

        let (_, _, resp_body) = self
            .execute_raw(
                Method::POST,
                Some(bucket),
                None,
                &[("delete", "")],
                headers,
                body_bytes,
            )
            .await?;
        xml::parse_delete_objects_result(&resp_body)
    }

    pub fn presign_get(
        &self,
        bucket: &str,
        key: &str,
        expires_secs: u64,
    ) -> Result<String, S3ClientError> {
        let url = self.build_url(Some(bucket), Some(key), &[])?;
        let creds = self.config.credentials.as_ref().ok_or_else(|| {
            S3ClientError::InvalidConfig("Cannot presign URL with --no-sign-request".to_string())
        })?;
        Ok(self.signer.presign_url(
            "GET",
            &url,
            creds,
            &self.config.region,
            expires_secs,
            Utc::now(),
        ))
    }
}

fn extract_object_head(headers: &HeaderMap) -> ObjectHead {
    let content_length = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let etag = headers
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim_matches('"').to_string());
    let last_modified = headers
        .get("last-modified")
        .and_then(|v| v.to_str().ok())
        .and_then(xml::parse_s3_timestamp);
    let storage_class = headers
        .get("x-amz-storage-class")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let version_id = headers
        .get("x-amz-version-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let checksum_crc32 = headers
        .get("x-amz-checksum-crc32")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let checksum_crc32c = headers
        .get("x-amz-checksum-crc32c")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let checksum_sha256 = headers
        .get("x-amz-checksum-sha256")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let mut metadata = HashMap::new();
    for (k, v) in headers.iter() {
        let name = k.as_str();
        if let Some(stripped) = name.strip_prefix("x-amz-meta-") {
            if let Ok(val_str) = v.to_str() {
                metadata.insert(stripped.to_string(), val_str.to_string());
            }
        }
    }

    ObjectHead {
        content_length,
        content_type,
        etag,
        last_modified,
        storage_class,
        version_id,
        metadata,
        checksum_crc32,
        checksum_crc32c,
        checksum_sha256,
    }
}

fn build_put_headers(opts: &PutObjectOptions) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let mut insert_opt = |name: &'static str, val_opt: &Option<String>| {
        if let Some(ref s) = val_opt {
            if let Ok(v) = HeaderValue::from_str(s) {
                headers.insert(name, v);
            }
        }
    };

    insert_opt("x-amz-storage-class", &opts.storage_class);
    insert_opt("x-amz-acl", &opts.acl);
    insert_opt("x-amz-server-side-encryption", &opts.sse);
    insert_opt(
        "x-amz-server-side-encryption-aws-kms-key-id",
        &opts.sse_kms_key_id,
    );
    insert_opt("cache-control", &opts.cache_control);
    insert_opt("expires", &opts.expires);
    insert_opt("content-type", &opts.content_type);
    insert_opt("content-encoding", &opts.content_encoding);
    insert_opt("content-disposition", &opts.content_disposition);

    for (k, v) in &opts.metadata {
        let header_name = if k.starts_with("x-amz-meta-") {
            k.to_ascii_lowercase()
        } else {
            format!("x-amz-meta-{}", k.to_ascii_lowercase())
        };
        if let (Ok(hn), Ok(hv)) = (
            HeaderName::from_bytes(header_name.as_bytes()),
            HeaderValue::from_str(v),
        ) {
            headers.insert(hn, hv);
        }
    }
    headers
}
