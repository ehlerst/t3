use crate::filter::GlobFilter;
use crate::lister::{compute_rel_key, stream_matching_s3_objects, MatchedS3Object};
use crate::select::SelectQuery;
use crate::uri::{compute_destination_key, split_wildcard, StorageLocation};
use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use t3_s3_client::{CompletedPartInfo, ObjectInfo, PutObjectOptions, S3Client};
use t3_storage::{
    safe_join, sanitize_relative_path, walk_local_files, BufferPool, DirCache, PositionalReader,
    PreallocatedWriter,
};
use t3_telemetry::{format_bytes, OutputContext};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
pub struct CopyOptions {
    pub concurrency: usize,
    pub part_size_bytes: u64,
    pub flatten: bool,
    pub no_clobber: bool,
    pub if_size_differ: bool,
    pub if_source_newer: bool,
    pub raw: bool,
    pub no_follow_symlinks: bool,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub version_id: Option<String>,
    pub source_region: Option<String>,
    pub destination_region: Option<String>,
    pub put_opts: PutObjectOptions,
    pub delete_source: bool,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            concurrency: 5,
            part_size_bytes: 50 * 1024 * 1024,
            flatten: false,
            no_clobber: false,
            if_size_differ: false,
            if_source_newer: false,
            raw: false,
            no_follow_symlinks: false,
            includes: Vec::new(),
            excludes: Vec::new(),
            version_id: None,
            source_region: None,
            destination_region: None,
            put_opts: PutObjectOptions::default(),
            delete_source: false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SyncOptions {
    pub copy_opts: CopyOptions,
    pub delete: bool,
    pub size_only: bool,
    pub exact_timestamps: bool,
}

#[derive(Debug, Clone, Default)]
pub struct LsOptions {
    pub etag: bool,
    pub humanize: bool,
    pub show_fullpath: bool,
    pub all_versions: bool,
}

#[derive(Debug, Clone, Default)]
pub struct RmOptions {
    pub raw: bool,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub version_id: Option<String>,
    pub all_versions: bool,
}

#[derive(Debug, Clone, Default)]
pub struct DuOptions {
    pub group_by_class: bool,
    pub humanize: bool,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub all_versions: bool,
}

#[derive(Clone)]
pub struct T3Engine {
    pub s3: S3Client,
    pub output: OutputContext,
    pub worker_sem: Arc<Semaphore>,
    pub buffer_pool: BufferPool,
    pub dir_cache: DirCache,
    pub num_workers: usize,
}

impl T3Engine {
    pub fn new(s3: S3Client, output: OutputContext, num_workers: isize) -> Self {
        let resolved_workers = if num_workers <= 0 {
            let cpus = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);
            let mult = if num_workers < 0 {
                (-num_workers) as usize
            } else {
                1
            };
            (cpus * mult).max(4)
        } else {
            num_workers as usize
        };

        let buffer_pool = BufferPool::new(8 * 1024 * 1024, (resolved_workers * 2).clamp(16, 256));
        Self {
            s3,
            output,
            worker_sem: Arc::new(Semaphore::new(resolved_workers)),
            buffer_pool,
            dir_cache: DirCache::new(),
            num_workers: resolved_workers,
        }
    }

    pub async fn mb(&self, target: &str) -> Result<()> {
        let loc = StorageLocation::parse(target)?;
        let StorageLocation::S3 { bucket, .. } = loc else {
            bail!("mb target must be an s3:// bucket URI");
        };
        if !self.output.dry_run {
            self.s3.create_bucket(&bucket).await?;
        }
        self.output.stats.record_op_success();
        self.output
            .emit_action("mb", &format!("s3://{bucket}"), None, None);
        Ok(())
    }

    pub async fn rb(&self, target: &str, force: bool) -> Result<()> {
        let loc = StorageLocation::parse(target)?;
        let StorageLocation::S3 { bucket, .. } = loc else {
            bail!("rb target must be an s3:// bucket URI");
        };
        if force {
            let rm_opts = RmOptions {
                all_versions: false,
                ..Default::default()
            };
            self.rm(&[format!("s3://{bucket}/*")], &rm_opts).await?;
        }
        if !self.output.dry_run {
            self.s3.delete_bucket(&bucket).await?;
        }
        self.output.stats.record_op_success();
        self.output
            .emit_action("rb", &format!("s3://{bucket}"), None, None);
        Ok(())
    }

    pub async fn bucket_version(&self, target: &str, set_status: Option<&str>) -> Result<()> {
        let loc = StorageLocation::parse(target)?;
        let StorageLocation::S3 { bucket, .. } = loc else {
            bail!("bucket-version target must be an s3:// bucket URI");
        };
        if let Some(status) = set_status {
            if !self.output.dry_run {
                self.s3.put_bucket_versioning(&bucket, status).await?;
            }
            self.output.stats.record_op_success();
            self.output.emit_action(
                "bucket-version",
                &format!("s3://{bucket}"),
                Some(status),
                None,
            );
        } else {
            let status = self.s3.get_bucket_versioning(&bucket).await?;
            self.output.stats.record_op_success();
            if self.output.json_mode {
                let val = json!({
                    "operation": "bucket-version",
                    "bucket": bucket,
                    "status": status,
                });
                println!("{val}");
            } else {
                println!("s3://{bucket} versioning status: {status}");
            }
        }
        Ok(())
    }

    pub async fn head(&self, target: &str, version_id: Option<&str>) -> Result<()> {
        let loc = StorageLocation::parse(target)?;
        let StorageLocation::S3 { bucket, key } = loc else {
            bail!("head target must be an s3:// URI");
        };

        if key.is_empty() {
            let region = self.s3.head_bucket(&bucket).await?;
            self.output.stats.record_op_success();
            let val = json!({
                "operation": "head",
                "bucket": bucket,
                "region": region.unwrap_or_else(|| self.s3.config().region.clone()),
            });
            println!("{val}");
            return Ok(());
        }

        let head = self.s3.head_object(&bucket, &key, version_id).await?;
        self.output.stats.record_op_success();
        let val = json!({
            "operation": "head",
            "key": format!("s3://{bucket}/{key}"),
            "size": head.content_length,
            "content_type": head.content_type,
            "etag": head.etag,
            "last_modified": head.last_modified,
            "storage_class": head.storage_class,
            "version_id": head.version_id,
            "metadata": head.metadata,
        });
        println!("{val}");
        Ok(())
    }

    pub fn presign(&self, target: &str, expires_secs: u64) -> Result<String> {
        let loc = StorageLocation::parse(target)?;
        let StorageLocation::S3 { bucket, key } = loc else {
            bail!("presign target must be an s3:// object URI");
        };
        if key.is_empty() {
            bail!("presign requires an object key (s3://bucket/key)");
        }
        let url = self.s3.presign_get(&bucket, &key, expires_secs)?;
        self.output.stats.record_op_success();
        if self.output.json_mode {
            println!(
                "{}",
                json!({
                    "operation": "presign",
                    "key": format!("s3://{bucket}/{key}"),
                    "url": url,
                })
            );
        } else {
            println!("{url}");
        }
        Ok(url)
    }

    pub async fn ls(&self, target: Option<&str>, opts: &LsOptions) -> Result<()> {
        let Some(raw_target) = target else {
            let buckets = self.s3.list_buckets().await?;
            self.output.stats.record_listed(buckets.len() as u64);
            for b in buckets {
                if self.output.json_mode {
                    println!(
                        "{}",
                        json!({
                            "operation": "ls",
                            "type": "bucket",
                            "name": b.name,
                            "creation_date": b.creation_date,
                        })
                    );
                } else {
                    let dt = b
                        .creation_date
                        .map(|d| d.format("%Y/%m/%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| "1970/01/01 00:00:00".to_string());
                    println!("{dt}  s3://{}", b.name);
                }
            }
            return Ok(());
        };

        let loc = StorageLocation::parse(raw_target)?;
        match loc {
            StorageLocation::S3 { bucket, key } => {
                // If no wildcard and listing bucket root or directory prefix without trailing wildcard,
                // list with Delimiter="/" when appropriate so common prefixes ("DIR") are shown like s5cmd!
                let sp = split_wildcard(&key, false);
                if !sp.has_glob && !opts.all_versions && (key.is_empty() || key.ends_with('/')) {
                    let mut token = None;
                    loop {
                        let page = self
                            .s3
                            .list_objects_page(
                                &bucket,
                                if key.is_empty() { None } else { Some(&key) },
                                Some("/"),
                                token.as_deref(),
                                1000,
                            )
                            .await?;
                        self.output.stats.record_listed(
                            (page.objects.len() + page.common_prefixes.len()) as u64,
                        );
                        for cp in &page.common_prefixes {
                            if self.output.json_mode {
                                println!(
                                    "{}",
                                    json!({
                                        "operation": "ls",
                                        "type": "directory",
                                        "key": format!("s3://{bucket}/{cp}"),
                                    })
                                );
                            } else {
                                let disp = if opts.show_fullpath {
                                    format!("s3://{bucket}/{cp}")
                                } else {
                                    cp.strip_prefix(&key).unwrap_or(cp).to_string()
                                };
                                println!("{:>19}  {:>12}  {}", "", "DIR", disp);
                            }
                        }
                        for obj in &page.objects {
                            self.print_ls_object(&bucket, &key, obj, opts);
                        }
                        if !page.is_truncated {
                            break;
                        }
                        token = page.next_continuation_token;
                        if token.is_none() {
                            break;
                        }
                    }
                    return Ok(());
                }

                let (tx, mut rx) = mpsc::channel(512);
                let client = self.s3.clone();
                let b_clone = bucket.clone();
                let k_clone = key.clone();
                let all_v = opts.all_versions;
                let st = Some(self.output.stats.clone());

                tokio::spawn(async move {
                    stream_matching_s3_objects(
                        client,
                        b_clone,
                        k_clone,
                        false,
                        all_v,
                        Vec::new(),
                        Vec::new(),
                        st,
                        tx,
                    )
                    .await;
                });

                while let Some(res) = rx.recv().await {
                    let matched = res?;
                    self.print_ls_object(&bucket, &sp.root_prefix, &matched.info, opts);
                }
                Ok(())
            }
            StorageLocation::Local { path } => {
                let entries = walk_local_files(&path, true)?;
                self.output.stats.record_listed(entries.len() as u64);
                for entry in entries {
                    let dt = entry
                        .modified
                        .map(|d| d.format("%Y/%m/%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| "1970/01/01 00:00:00".to_string());
                    let sz = format_bytes(entry.size, opts.humanize);
                    println!("{dt}  {sz}  {}", entry.abs_path.display());
                }
                Ok(())
            }
        }
    }

    fn print_ls_object(&self, bucket: &str, prefix: &str, obj: &ObjectInfo, opts: &LsOptions) {
        if self.output.json_mode {
            println!(
                "{}",
                json!({
                    "operation": "ls",
                    "type": "file",
                    "key": format!("s3://{bucket}/{}", obj.key),
                    "size": obj.size,
                    "etag": obj.etag,
                    "last_modified": obj.last_modified,
                    "storage_class": obj.storage_class,
                    "version_id": obj.version_id,
                })
            );
            return;
        }

        let dt = obj
            .last_modified
            .map(|d| d.format("%Y/%m/%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "1970/01/01 00:00:00".to_string());
        let sz = format_bytes(obj.size, opts.humanize);
        let disp_key = if opts.show_fullpath {
            format!("s3://{bucket}/{}", obj.key)
        } else if !prefix.is_empty() && obj.key != prefix {
            obj.key.strip_prefix(prefix).unwrap_or(&obj.key).to_string()
        } else {
            obj.key.clone()
        };

        let etag_col = if opts.etag {
            format!("  {:<34}", obj.etag.as_deref().unwrap_or(""))
        } else {
            String::new()
        };

        let ver_col = match obj.version_id {
            Some(ref v) if opts.all_versions => format!("  {v}"),
            _ => String::new(),
        };

        println!("{dt}{etag_col}  {sz}  {disp_key}{ver_col}");
    }

    pub async fn du(&self, target: &str, opts: &DuOptions) -> Result<()> {
        let loc = StorageLocation::parse(target)?;
        let StorageLocation::S3 { bucket, key } = loc else {
            bail!("du target must be an s3:// URI");
        };

        let (tx, mut rx) = mpsc::channel(512);
        let client = self.s3.clone();
        let b_clone = bucket.clone();
        let k_clone = key.clone();
        let inc = opts.includes.clone();
        let exc = opts.excludes.clone();
        let all_v = opts.all_versions;
        let st = Some(self.output.stats.clone());

        tokio::spawn(async move {
            stream_matching_s3_objects(client, b_clone, k_clone, false, all_v, inc, exc, st, tx)
                .await;
        });

        let mut total_bytes = 0u64;
        let mut total_count = 0u64;
        let mut by_class: HashMap<String, (u64, u64)> = HashMap::new();

        while let Some(res) = rx.recv().await {
            let matched = res?;
            total_bytes += matched.info.size;
            total_count += 1;
            if opts.group_by_class {
                let sc = matched
                    .info
                    .storage_class
                    .unwrap_or_else(|| "STANDARD".to_string());
                let entry = by_class.entry(sc).or_insert((0, 0));
                entry.0 += matched.info.size;
                entry.1 += 1;
            }
        }

        if self.output.json_mode {
            println!(
                "{}",
                json!({
                    "operation": "du",
                    "target": target,
                    "size": total_bytes,
                    "count": total_count,
                    "by_storage_class": by_class,
                })
            );
        } else if opts.group_by_class {
            for (sc, (b, c)) in &by_class {
                println!(
                    "{} bytes in {} objects: {} [{}]",
                    format_bytes(*b, opts.humanize).trim(),
                    c,
                    target,
                    sc
                );
            }
        } else {
            println!(
                "{} bytes in {} objects: {}",
                format_bytes(total_bytes, opts.humanize).trim(),
                total_count,
                target
            );
        }
        Ok(())
    }

    pub async fn cat(&self, targets: &[String], version_id: Option<&str>) -> Result<()> {
        for raw_target in targets {
            let loc = StorageLocation::parse(raw_target)?;
            let StorageLocation::S3 { bucket, key } = loc else {
                bail!("cat target must be an s3:// object URI");
            };
            let sp = split_wildcard(&key, false);
            if !sp.has_glob {
                let (_, bytes) = self.s3.get_object(&bucket, &key, None, version_id).await?;
                self.output.stats.record_transfer(bytes.len() as u64, 1);
                let mut out = io::stdout().lock();
                out.write_all(&bytes)?;
                out.flush()?;
            } else {
                let (tx, mut rx) = mpsc::channel(64);
                let client = self.s3.clone();
                let b_clone = bucket.clone();
                let k_clone = key.clone();
                let st = Some(self.output.stats.clone());
                tokio::spawn(async move {
                    stream_matching_s3_objects(
                        client,
                        b_clone,
                        k_clone,
                        false,
                        false,
                        Vec::new(),
                        Vec::new(),
                        st,
                        tx,
                    )
                    .await;
                });
                while let Some(res) = rx.recv().await {
                    let matched = res?;
                    let (_, bytes) = self
                        .s3
                        .get_object(&bucket, &matched.info.key, None, None)
                        .await?;
                    self.output.stats.record_transfer(bytes.len() as u64, 1);
                    let mut out = io::stdout().lock();
                    out.write_all(&bytes)?;
                    out.flush()?;
                }
            }
        }
        Ok(())
    }

    pub async fn pipe(&self, target: &str, opts: &CopyOptions) -> Result<()> {
        let loc = StorageLocation::parse(target)?;
        let StorageLocation::S3 { bucket, key } = loc else {
            bail!("pipe destination must be an s3:// object URI");
        };
        if key.is_empty() {
            bail!("pipe destination requires an S3 object key");
        }

        let part_size = opts.part_size_bytes.max(5 * 1024 * 1024) as usize;
        let mut stdin = io::stdin();
        let mut first_chunk = Vec::with_capacity(part_size.min(8 * 1024 * 1024));
        let mut temp_buf = [0u8; 64 * 1024];

        while first_chunk.len() < part_size {
            let n = stdin.read(&mut temp_buf)?;
            if n == 0 {
                break;
            }
            first_chunk.extend_from_slice(&temp_buf[..n]);
        }

        // If stdin EOF was reached within the first part, execute a single fast PutObject!
        if first_chunk.len() < part_size {
            let total_len = first_chunk.len() as u64;
            if !self.output.dry_run {
                self.s3
                    .put_object(&bucket, &key, Bytes::from(first_chunk), &opts.put_opts)
                    .await?;
            }
            self.output.stats.record_transfer(total_len, 1);
            self.output.emit_action(
                "pipe",
                "stdin",
                Some(&format!("s3://{bucket}/{key}")),
                Some(total_len),
            );
            return Ok(());
        }

        // Otherwise stream via parallel multipart upload
        let upload_id = self
            .s3
            .create_multipart_upload(&bucket, &key, &opts.put_opts)
            .await?;
        let part_sem = Arc::new(Semaphore::new(opts.concurrency.max(1)));
        let mut set = JoinSet::new();
        let mut part_number = 1i32;
        let mut total_bytes = first_chunk.len() as u64;

        {
            let c = self.s3.clone();
            let b = bucket.clone();
            let k = key.clone();
            let uid = upload_id.clone();
            let chunk_bytes = Bytes::from(first_chunk);
            let sem = part_sem.clone();
            set.spawn(async move {
                let _p = sem.acquire().await;
                let etag = c.upload_part(&b, &k, &uid, 1, chunk_bytes).await?;
                Ok::<CompletedPartInfo, anyhow::Error>(CompletedPartInfo {
                    part_number: 1,
                    etag,
                })
            });
        }

        loop {
            let mut chunk = Vec::with_capacity(part_size.min(8 * 1024 * 1024));
            while chunk.len() < part_size {
                let n = stdin.read(&mut temp_buf)?;
                if n == 0 {
                    break;
                }
                chunk.extend_from_slice(&temp_buf[..n]);
            }
            if chunk.is_empty() {
                break;
            }
            part_number += 1;
            total_bytes += chunk.len() as u64;
            let pnum = part_number;
            let c = self.s3.clone();
            let b = bucket.clone();
            let k = key.clone();
            let uid = upload_id.clone();
            let chunk_bytes = Bytes::from(chunk);
            let permit = part_sem.clone().acquire_owned().await?;
            set.spawn(async move {
                let _p = permit;
                let etag = c.upload_part(&b, &k, &uid, pnum, chunk_bytes).await?;
                Ok::<CompletedPartInfo, anyhow::Error>(CompletedPartInfo {
                    part_number: pnum,
                    etag,
                })
            });
        }

        let mut completed_parts = Vec::with_capacity(part_number as usize);
        while let Some(res) = set.join_next().await {
            match res? {
                Ok(part) => completed_parts.push(part),
                Err(e) => {
                    let _ = self
                        .s3
                        .abort_multipart_upload(&bucket, &key, &upload_id)
                        .await;
                    return Err(e);
                }
            }
        }

        self.s3
            .complete_multipart_upload(&bucket, &key, &upload_id, completed_parts)
            .await?;
        self.output
            .stats
            .record_transfer(total_bytes, part_number as u64);
        self.output.emit_action(
            "pipe",
            "stdin",
            Some(&format!("s3://{bucket}/{key}")),
            Some(total_bytes),
        );
        Ok(())
    }

    pub async fn select(
        &self,
        target: &str,
        query_sql: &str,
        input_format: &str,
        output_format: &str,
        includes: &[String],
        excludes: &[String],
    ) -> Result<()> {
        let loc = StorageLocation::parse(target)?;
        let StorageLocation::S3 { bucket, key } = loc else {
            bail!("select target must be an s3:// URI");
        };
        let query = Arc::new(SelectQuery::parse(query_sql));
        let sp = split_wildcard(&key, false);

        if !sp.has_glob {
            let (_, bytes) = self.s3.get_object(&bucket, &key, None, None).await?;
            self.output.stats.record_transfer(bytes.len() as u64, 1);
            let rows = query.execute_on_bytes(&bytes, input_format, output_format);
            let mut out = io::stdout().lock();
            for r in rows {
                writeln!(out, "{r}")?;
            }
            return Ok(());
        }

        let (tx, mut rx) = mpsc::channel(256);
        let client = self.s3.clone();
        let b_clone = bucket.clone();
        let k_clone = key.clone();
        let inc = includes.to_vec();
        let exc = excludes.to_vec();
        let st = Some(self.output.stats.clone());

        tokio::spawn(async move {
            stream_matching_s3_objects(client, b_clone, k_clone, false, false, inc, exc, st, tx)
                .await;
        });

        let mut set = JoinSet::new();
        while let Some(res) = rx.recv().await {
            let matched = res?;
            let permit = self.worker_sem.clone().acquire_owned().await?;
            let c = self.s3.clone();
            let b = bucket.clone();
            let q = query.clone();
            let in_fmt = input_format.to_string();
            let out_fmt = output_format.to_string();
            let st_ref = self.output.stats.clone();
            set.spawn(async move {
                let _p = permit;
                let (_, bytes) = c.get_object(&b, &matched.info.key, None, None).await?;
                st_ref.record_transfer(bytes.len() as u64, 1);
                Ok::<Vec<String>, anyhow::Error>(q.execute_on_bytes(&bytes, &in_fmt, &out_fmt))
            });
        }

        while let Some(res) = set.join_next().await {
            let rows = res??;
            let mut out = io::stdout().lock();
            for r in rows {
                writeln!(out, "{r}")?;
            }
        }
        Ok(())
    }

    pub async fn rm(&self, targets: &[String], opts: &RmOptions) -> Result<()> {
        let mut set = JoinSet::new();

        for raw_target in targets {
            let loc = StorageLocation::parse(raw_target)?;
            match loc {
                StorageLocation::S3 { bucket, key } => {
                    let sp = split_wildcard(&key, opts.raw);
                    if !sp.has_glob && !opts.all_versions && !key.ends_with('/') {
                        // Single object delete
                        let eng = self.clone();
                        let b = bucket.clone();
                        let k = key.clone();
                        let vid = opts.version_id.clone();
                        let permit = self.worker_sem.clone().acquire_owned().await?;
                        set.spawn(async move {
                            let _p = permit;
                            if !eng.output.dry_run {
                                eng.s3.delete_object(&b, &k, vid.as_deref()).await?;
                            }
                            eng.output.stats.record_deleted(1);
                            eng.output
                                .emit_action("rm", &format!("s3://{b}/{k}"), None, None);
                            Ok::<(), anyhow::Error>(())
                        });
                    } else {
                        // Pipelined 1,000-key batch deleter!
                        let (tx, mut rx) = mpsc::channel(2048);
                        let client = self.s3.clone();
                        let b_clone = bucket.clone();
                        let k_clone = key.clone();
                        let raw_mode = opts.raw;
                        let all_v = opts.all_versions;
                        let inc = opts.includes.clone();
                        let exc = opts.excludes.clone();
                        let st = Some(self.output.stats.clone());

                        tokio::spawn(async move {
                            stream_matching_s3_objects(
                                client, b_clone, k_clone, raw_mode, all_v, inc, exc, st, tx,
                            )
                            .await;
                        });

                        let mut batch: Vec<(String, Option<String>)> = Vec::with_capacity(1000);
                        while let Some(res) = rx.recv().await {
                            let matched = res?;
                            batch.push((matched.info.key, matched.info.version_id));
                            if batch.len() >= 1000 {
                                let chunk = std::mem::replace(&mut batch, Vec::with_capacity(1000));
                                self.spawn_delete_batch(&mut set, &bucket, chunk).await?;
                            }
                        }
                        if !batch.is_empty() {
                            self.spawn_delete_batch(&mut set, &bucket, batch).await?;
                        }
                    }
                }
                StorageLocation::Local { path } => {
                    if path.is_file() {
                        if !self.output.dry_run {
                            fs::remove_file(&path)?;
                        }
                        self.output.stats.record_deleted(1);
                        self.output
                            .emit_action("rm", &path.display().to_string(), None, None);
                    }
                }
            }
        }

        let mut first_err = None;
        while let Some(res) = set.join_next().await {
            if let Err(e) = res? {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        Ok(())
    }

    async fn spawn_delete_batch(
        &self,
        set: &mut JoinSet<Result<()>>,
        bucket: &str,
        batch: Vec<(String, Option<String>)>,
    ) -> Result<()> {
        let permit = self.worker_sem.clone().acquire_owned().await?;
        let eng = self.clone();
        let b = bucket.to_string();
        set.spawn(async move {
            let _p = permit;
            let count = batch.len() as u64;
            if !eng.output.dry_run {
                let out = eng.s3.delete_objects(&b, &batch).await?;
                if !out.errors.is_empty() {
                    for (k, msg) in &out.errors {
                        eng.output.emit_error("rm", &format!("s3://{b}/{k}"), msg);
                    }
                    bail!("Failed to delete {} objects in s3://{b}", out.errors.len());
                }
            }
            eng.output.stats.record_deleted(count);
            for (k, _) in &batch {
                eng.output
                    .emit_action("rm", &format!("s3://{b}/{k}"), None, None);
            }
            Ok(())
        });
        Ok(())
    }

    pub async fn cp(&self, src: &str, dst: &str, opts: &CopyOptions) -> Result<()> {
        let src_loc = StorageLocation::parse(src)?;
        let dst_loc = StorageLocation::parse(dst)?;
        let op_label = if opts.delete_source { "mv" } else { "cp" };

        match (src_loc, dst_loc) {
            (
                StorageLocation::Local { path: src_path },
                StorageLocation::S3 {
                    bucket: dst_bucket,
                    key: dst_key,
                },
            ) => {
                self.cp_local_to_s3(&src_path, &dst_bucket, &dst_key, opts, op_label)
                    .await
            }
            (
                StorageLocation::S3 {
                    bucket: src_bucket,
                    key: src_key,
                },
                StorageLocation::Local { path: dst_path },
            ) => {
                self.cp_s3_to_local(&src_bucket, &src_key, &dst_path, opts, op_label)
                    .await
            }
            (
                StorageLocation::S3 {
                    bucket: src_bucket,
                    key: src_key,
                },
                StorageLocation::S3 {
                    bucket: dst_bucket,
                    key: dst_key,
                },
            ) => {
                self.cp_s3_to_s3(&src_bucket, &src_key, &dst_bucket, &dst_key, opts, op_label)
                    .await
            }
            (
                StorageLocation::Local { path: src_path },
                StorageLocation::Local { path: dst_path },
            ) => {
                self.cp_local_to_local(&src_path, &dst_path, opts, op_label)
                    .await
            }
        }
    }

    async fn cp_local_to_s3(
        &self,
        src_path: &Path,
        dst_bucket: &str,
        dst_key: &str,
        opts: &CopyOptions,
        op_label: &'static str,
    ) -> Result<()> {
        let src_str = src_path.to_string_lossy();
        let sp = split_wildcard(&src_str, opts.raw);
        let walk_root = if sp.has_glob {
            let p = Path::new(&sp.root_prefix);
            if p.as_os_str().is_empty() {
                Path::new(".")
            } else {
                p
            }
        } else {
            src_path
        };

        let filter =
            GlobFilter::compile(sp.glob_pattern.as_deref(), &opts.includes, &opts.excludes)?;
        let entries = walk_local_files(walk_root, !opts.no_follow_symlinks)?;
        let is_multi = sp.has_glob || walk_root.is_dir() || entries.len() > 1;

        let mut set = JoinSet::new();
        for entry in entries {
            let full_norm = entry.abs_path.to_string_lossy();
            if !filter.matches(&full_norm, &entry.rel_path) {
                continue;
            }
            let target_key =
                compute_destination_key(dst_key, &entry.rel_path, opts.flatten, is_multi);

            let permit = self.worker_sem.clone().acquire_owned().await?;
            let eng = self.clone();
            let b = dst_bucket.to_string();
            let opts_clone = opts.clone();
            set.spawn(async move {
                let _p = permit;
                eng.upload_single_file(
                    &entry.abs_path,
                    entry.size,
                    entry.modified,
                    &b,
                    &target_key,
                    &opts_clone,
                    op_label,
                )
                .await
            });
        }

        collect_join_set(&mut set).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upload_single_file(
        &self,
        local_path: &Path,
        size: u64,
        src_mtime: Option<DateTime<Utc>>,
        bucket: &str,
        key: &str,
        opts: &CopyOptions,
        op_label: &str,
    ) -> Result<()> {
        if !self
            .should_overwrite_s3(bucket, key, size, src_mtime, opts)
            .await?
        {
            return Ok(());
        }

        let src_disp = local_path.display().to_string();
        let dst_disp = format!("s3://{bucket}/{key}");

        if self.output.dry_run {
            self.output
                .emit_action(op_label, &src_disp, Some(&dst_disp), Some(size));
            return Ok(());
        }

        let part_size = opts.part_size_bytes.max(5 * 1024 * 1024);
        if size <= part_size {
            let data = fs::read(local_path)
                .with_context(|| format!("Failed to read {}", local_path.display()))?;
            self.s3
                .put_object(bucket, key, Bytes::from(data), &opts.put_opts)
                .await?;
            self.output.stats.record_transfer(size, 1);
        } else {
            let reader = PositionalReader::open(local_path)?;
            let upload_id = self
                .s3
                .create_multipart_upload(bucket, key, &opts.put_opts)
                .await?;
            let num_parts = size.div_ceil(part_size);
            let part_sem = Arc::new(Semaphore::new(opts.concurrency.max(1)));
            let mut part_set = JoinSet::new();

            for part_idx in 0..num_parts {
                let offset = part_idx * part_size;
                let chunk_len = (size - offset).min(part_size) as usize;
                let pnum = (part_idx + 1) as i32;
                let permit = part_sem.clone().acquire_owned().await?;
                let r = reader.clone();
                let c = self.s3.clone();
                let b = bucket.to_string();
                let k = key.to_string();
                let uid = upload_id.clone();

                part_set.spawn(async move {
                    let _p = permit;
                    let mut buf = vec![0u8; chunk_len];
                    r.read_at(offset, &mut buf)?;
                    let body = Bytes::from(buf);
                    let etag = c.upload_part(&b, &k, &uid, pnum, body).await?;
                    Ok::<CompletedPartInfo, anyhow::Error>(CompletedPartInfo {
                        part_number: pnum,
                        etag,
                    })
                });
            }

            let mut completed = Vec::with_capacity(num_parts as usize);
            while let Some(res) = part_set.join_next().await {
                match res? {
                    Ok(info) => completed.push(info),
                    Err(e) => {
                        let _ = self
                            .s3
                            .abort_multipart_upload(bucket, key, &upload_id)
                            .await;
                        return Err(e);
                    }
                }
            }
            self.s3
                .complete_multipart_upload(bucket, key, &upload_id, completed)
                .await?;
            self.output.stats.record_transfer(size, num_parts);
        }

        if opts.delete_source {
            fs::remove_file(local_path)?;
            self.output.stats.record_deleted(1);
        }

        self.output
            .emit_action(op_label, &src_disp, Some(&dst_disp), Some(size));
        Ok(())
    }

    async fn cp_s3_to_local(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_path: &Path,
        opts: &CopyOptions,
        op_label: &'static str,
    ) -> Result<()> {
        let sp = split_wildcard(src_key, opts.raw);
        let dst_is_dir = dst_path.is_dir()
            || dst_path.to_string_lossy().ends_with('/')
            || sp.has_glob
            || src_key.is_empty()
            || src_key.ends_with('/');

        // Fast path: single explicit object key (no glob, not ending with `/`)
        if !sp.has_glob && !src_key.is_empty() && !src_key.ends_with('/') {
            if let Ok(head) = self
                .s3
                .head_object(src_bucket, src_key, opts.version_id.as_deref())
                .await
            {
                let rel_name = src_key.rsplit('/').next().unwrap_or(src_key);
                let final_dst = if dst_is_dir {
                    safe_join(dst_path, rel_name)?
                } else {
                    if let Some(rel) = dst_path.to_str() {
                        if rel.contains("..") {
                            let _ = sanitize_relative_path(rel)?;
                        }
                    }
                    dst_path.to_path_buf()
                };
                return self
                    .download_single_object(
                        src_bucket,
                        src_key,
                        head.content_length,
                        head.last_modified,
                        &final_dst,
                        opts,
                        op_label,
                    )
                    .await;
            }
        }

        let (tx, mut rx) = mpsc::channel(512);
        let client = self.s3.clone();
        let b_clone = src_bucket.to_string();
        let k_clone = src_key.to_string();
        let raw_mode = opts.raw;
        let inc = opts.includes.clone();
        let exc = opts.excludes.clone();
        let st = Some(self.output.stats.clone());

        tokio::spawn(async move {
            stream_matching_s3_objects(client, b_clone, k_clone, raw_mode, false, inc, exc, st, tx)
                .await;
        });

        let mut set = JoinSet::new();
        while let Some(res) = rx.recv().await {
            let matched = res?;
            let rel_for_dst = if opts.flatten {
                matched
                    .rel_key
                    .rsplit('/')
                    .next()
                    .unwrap_or(&matched.rel_key)
            } else {
                &matched.rel_key
            };

            let target_file = if dst_is_dir {
                safe_join(dst_path, rel_for_dst)?
            } else {
                let _ = sanitize_relative_path(rel_for_dst)?;
                dst_path.to_path_buf()
            };

            let permit = self.worker_sem.clone().acquire_owned().await?;
            let eng = self.clone();
            let b = src_bucket.to_string();
            let opts_clone = opts.clone();
            set.spawn(async move {
                let _p = permit;
                eng.download_single_object(
                    &b,
                    &matched.info.key,
                    matched.info.size,
                    matched.info.last_modified,
                    &target_file,
                    &opts_clone,
                    op_label,
                )
                .await
            });
        }

        collect_join_set(&mut set).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn download_single_object(
        &self,
        bucket: &str,
        key: &str,
        size: u64,
        src_mtime: Option<DateTime<Utc>>,
        dst_file: &Path,
        opts: &CopyOptions,
        op_label: &str,
    ) -> Result<()> {
        // Always guard against malicious S3 keys containing `..`
        let _ = sanitize_relative_path(key)?;

        if !self.should_overwrite_local(dst_file, size, src_mtime, opts) {
            return Ok(());
        }

        let src_disp = format!("s3://{bucket}/{key}");
        let dst_disp = dst_file.display().to_string();

        if self.output.dry_run {
            self.output
                .emit_action(op_label, &src_disp, Some(&dst_disp), Some(size));
            return Ok(());
        }

        let part_size = opts.part_size_bytes.max(5 * 1024 * 1024);
        if size <= part_size || opts.concurrency <= 1 {
            let (_, bytes) = self
                .s3
                .get_object(bucket, key, None, opts.version_id.as_deref())
                .await?;
            let writer = PreallocatedWriter::create(dst_file, bytes.len() as u64, &self.dir_cache)?;
            writer.write_at(0, &bytes)?;
            self.output.stats.record_transfer(bytes.len() as u64, 1);
        } else {
            let writer = PreallocatedWriter::create(dst_file, size, &self.dir_cache)?;
            let num_parts = size.div_ceil(part_size);
            let part_sem = Arc::new(Semaphore::new(opts.concurrency.max(1)));
            let mut part_set = JoinSet::new();

            for part_idx in 0..num_parts {
                let start = part_idx * part_size;
                let end = (start + part_size - 1).min(size - 1);
                let permit = part_sem.clone().acquire_owned().await?;
                let c = self.s3.clone();
                let b = bucket.to_string();
                let k = key.to_string();
                let vid = opts.version_id.clone();
                let w = writer.clone();

                part_set.spawn(async move {
                    let _p = permit;
                    let (_, chunk) = c
                        .get_object(&b, &k, Some((start, end)), vid.as_deref())
                        .await?;
                    w.write_at(start, &chunk)?;
                    Ok::<(), anyhow::Error>(())
                });
            }

            collect_join_set(&mut part_set).await?;
            self.output.stats.record_transfer(size, num_parts);
        }

        if opts.delete_source {
            self.s3
                .delete_object(bucket, key, opts.version_id.as_deref())
                .await?;
            self.output.stats.record_deleted(1);
        }

        self.output
            .emit_action(op_label, &src_disp, Some(&dst_disp), Some(size));
        Ok(())
    }

    async fn cp_s3_to_s3(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        opts: &CopyOptions,
        op_label: &'static str,
    ) -> Result<()> {
        let sp = split_wildcard(src_key, opts.raw);

        if !sp.has_glob && !src_key.is_empty() && !src_key.ends_with('/') {
            if let Ok(head) = self
                .s3
                .head_object(src_bucket, src_key, opts.version_id.as_deref())
                .await
            {
                let rel_name = src_key.rsplit('/').next().unwrap_or(src_key);
                let target_key = compute_destination_key(dst_key, rel_name, opts.flatten, false);
                return self
                    .copy_single_s3_object(
                        src_bucket,
                        src_key,
                        head.content_length,
                        head.last_modified,
                        dst_bucket,
                        &target_key,
                        opts,
                        op_label,
                    )
                    .await;
            }
        }

        let (tx, mut rx) = mpsc::channel(512);
        let client = self.s3.clone();
        let b_clone = src_bucket.to_string();
        let k_clone = src_key.to_string();
        let raw_mode = opts.raw;
        let inc = opts.includes.clone();
        let exc = opts.excludes.clone();
        let st = Some(self.output.stats.clone());

        tokio::spawn(async move {
            stream_matching_s3_objects(client, b_clone, k_clone, raw_mode, false, inc, exc, st, tx)
                .await;
        });

        let mut set = JoinSet::new();
        while let Some(res) = rx.recv().await {
            let matched = res?;
            let target_key = compute_destination_key(dst_key, &matched.rel_key, opts.flatten, true);
            let permit = self.worker_sem.clone().acquire_owned().await?;
            let eng = self.clone();
            let sb = src_bucket.to_string();
            let db = dst_bucket.to_string();
            let opts_clone = opts.clone();
            set.spawn(async move {
                let _p = permit;
                eng.copy_single_s3_object(
                    &sb,
                    &matched.info.key,
                    matched.info.size,
                    matched.info.last_modified,
                    &db,
                    &target_key,
                    &opts_clone,
                    op_label,
                )
                .await
            });
        }

        collect_join_set(&mut set).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn copy_single_s3_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        size: u64,
        src_mtime: Option<DateTime<Utc>>,
        dst_bucket: &str,
        dst_key: &str,
        opts: &CopyOptions,
        op_label: &str,
    ) -> Result<()> {
        if !self
            .should_overwrite_s3(dst_bucket, dst_key, size, src_mtime, opts)
            .await?
        {
            return Ok(());
        }

        let src_disp = format!("s3://{src_bucket}/{src_key}");
        let dst_disp = format!("s3://{dst_bucket}/{dst_key}");

        if self.output.dry_run {
            self.output
                .emit_action(op_label, &src_disp, Some(&dst_disp), Some(size));
            return Ok(());
        }

        let part_size = opts.part_size_bytes.max(5 * 1024 * 1024);
        let cross_region = opts.source_region.is_some()
            && opts.destination_region.is_some()
            && opts.source_region != opts.destination_region;

        if size <= part_size && !cross_region {
            self.s3
                .copy_object(
                    src_bucket,
                    src_key,
                    opts.version_id.as_deref(),
                    dst_bucket,
                    dst_key,
                    &opts.put_opts,
                )
                .await?;
            self.output.stats.record_transfer(size, 1);
        } else {
            // Parallel multipart streaming copy (handles objects > 5 GiB & cross-endpoint copies, fixing s5cmd #29!)
            let upload_id = self
                .s3
                .create_multipart_upload(dst_bucket, dst_key, &opts.put_opts)
                .await?;
            let num_parts = size.div_ceil(part_size);
            let part_sem = Arc::new(Semaphore::new(opts.concurrency.max(1)));
            let mut part_set = JoinSet::new();

            for part_idx in 0..num_parts {
                let start = part_idx * part_size;
                let end = (start + part_size - 1).min(size - 1);
                let pnum = (part_idx + 1) as i32;
                let permit = part_sem.clone().acquire_owned().await?;
                let c = self.s3.clone();
                let sb = src_bucket.to_string();
                let sk = src_key.to_string();
                let db = dst_bucket.to_string();
                let dk = dst_key.to_string();
                let uid = upload_id.clone();
                let vid = opts.version_id.clone();

                part_set.spawn(async move {
                    let _p = permit;
                    let (_, data) = c
                        .get_object(&sb, &sk, Some((start, end)), vid.as_deref())
                        .await?;
                    let etag = c.upload_part(&db, &dk, &uid, pnum, data).await?;
                    Ok::<CompletedPartInfo, anyhow::Error>(CompletedPartInfo {
                        part_number: pnum,
                        etag,
                    })
                });
            }

            let mut completed = Vec::with_capacity(num_parts as usize);
            while let Some(res) = part_set.join_next().await {
                match res? {
                    Ok(info) => completed.push(info),
                    Err(e) => {
                        let _ = self
                            .s3
                            .abort_multipart_upload(dst_bucket, dst_key, &upload_id)
                            .await;
                        return Err(e);
                    }
                }
            }
            self.s3
                .complete_multipart_upload(dst_bucket, dst_key, &upload_id, completed)
                .await?;
            self.output.stats.record_transfer(size, num_parts);
        }

        if opts.delete_source {
            self.s3
                .delete_object(src_bucket, src_key, opts.version_id.as_deref())
                .await?;
            self.output.stats.record_deleted(1);
        }

        self.output
            .emit_action(op_label, &src_disp, Some(&dst_disp), Some(size));
        Ok(())
    }

    async fn cp_local_to_local(
        &self,
        src_path: &Path,
        dst_path: &Path,
        opts: &CopyOptions,
        op_label: &'static str,
    ) -> Result<()> {
        let entries = walk_local_files(src_path, !opts.no_follow_symlinks)?;
        let dst_is_dir =
            dst_path.is_dir() || dst_path.to_string_lossy().ends_with('/') || entries.len() > 1;
        for entry in entries {
            let target = if dst_is_dir {
                safe_join(dst_path, &entry.rel_path)?
            } else {
                dst_path.to_path_buf()
            };
            if !self.should_overwrite_local(&target, entry.size, entry.modified, opts) {
                continue;
            }
            if !self.output.dry_run {
                self.dir_cache.ensure_parent(&target)?;
                fs::copy(&entry.abs_path, &target)?;
                if opts.delete_source {
                    fs::remove_file(&entry.abs_path)?;
                    self.output.stats.record_deleted(1);
                }
            }
            self.output.stats.record_transfer(entry.size, 1);
            self.output.emit_action(
                op_label,
                &entry.abs_path.display().to_string(),
                Some(&target.display().to_string()),
                Some(entry.size),
            );
        }
        Ok(())
    }

    async fn should_overwrite_s3(
        &self,
        bucket: &str,
        key: &str,
        src_size: u64,
        src_mtime: Option<DateTime<Utc>>,
        opts: &CopyOptions,
    ) -> Result<bool> {
        if !opts.no_clobber && !opts.if_size_differ && !opts.if_source_newer {
            return Ok(true);
        }
        match self.s3.head_object(bucket, key, None).await {
            Ok(dst_head) => {
                if opts.no_clobber {
                    return Ok(false);
                }
                if opts.if_size_differ && dst_head.content_length != src_size {
                    return Ok(true);
                }
                if opts.if_source_newer {
                    if let (Some(sm), Some(dm)) = (src_mtime, dst_head.last_modified) {
                        if sm > dm {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            }
            Err(e) if e.is_not_found() => Ok(true),
            Err(e) => Err(e.into()),
        }
    }

    fn should_overwrite_local(
        &self,
        dst_file: &Path,
        src_size: u64,
        src_mtime: Option<DateTime<Utc>>,
        opts: &CopyOptions,
    ) -> bool {
        if !opts.no_clobber && !opts.if_size_differ && !opts.if_source_newer {
            return true;
        }
        let Ok(meta) = fs::metadata(dst_file) else {
            return true;
        };
        if opts.no_clobber {
            return false;
        }
        if opts.if_size_differ && meta.len() != src_size {
            return true;
        }
        if opts.if_source_newer {
            if let (Some(sm), Ok(dm_sys)) = (src_mtime, meta.modified()) {
                let dm = DateTime::<Utc>::from(dm_sys);
                if sm > dm {
                    return true;
                }
            }
        }
        false
    }

    /// High-speed concurrent `sync` engine.
    /// Lists source and destination concurrently, streams diffs into the worker pool, and
    /// supports `--delete` while strictly respecting `--include` and `--exclude` filters (fixing s5cmd #815 / #852).
    pub async fn sync(&self, src: &str, dst: &str, opts: &SyncOptions) -> Result<()> {
        let src_loc = StorageLocation::parse(src)?;
        let dst_loc = StorageLocation::parse(dst)?;
        let filter = GlobFilter::compile(None, &opts.copy_opts.includes, &opts.copy_opts.excludes)?;

        let (src_map, dst_map) = tokio::try_join!(
            self.collect_sync_entries(&src_loc, &filter, !opts.copy_opts.no_follow_symlinks),
            self.collect_sync_entries(&dst_loc, &filter, !opts.copy_opts.no_follow_symlinks)
        )?;

        let seen_rel_keys: DashMap<String, ()> = DashMap::with_capacity(src_map.len());
        let mut set = JoinSet::new();

        for (rel_key, src_item) in src_map {
            seen_rel_keys.insert(rel_key.clone(), ());
            let needs_copy = match dst_map.get(&rel_key) {
                None => true,
                Some(dst_item) => should_sync_item(&src_item, dst_item, opts),
            };

            if !needs_copy {
                continue;
            }

            let permit = self.worker_sem.clone().acquire_owned().await?;
            let eng = self.clone();
            let c_opts = opts.copy_opts.clone();
            let dst_loc_clone = dst_loc.clone();

            set.spawn(async move {
                let _p = permit;
                match (&src_item.origin, &dst_loc_clone) {
                    (
                        SyncOrigin::Local(abs_path),
                        StorageLocation::S3 {
                            bucket: dst_bucket,
                            key: dst_prefix,
                        },
                    ) => {
                        let dst_k = compute_destination_key(dst_prefix, &rel_key, false, true);
                        eng.upload_single_file(
                            abs_path,
                            src_item.size,
                            src_item.modified,
                            dst_bucket,
                            &dst_k,
                            &c_opts,
                            "cp",
                        )
                        .await
                    }
                    (
                        SyncOrigin::S3 {
                            bucket: src_bucket,
                            key: src_k,
                        },
                        StorageLocation::Local { path: dst_root },
                    ) => {
                        let target_file = safe_join(dst_root, &rel_key)?;
                        eng.download_single_object(
                            src_bucket,
                            src_k,
                            src_item.size,
                            src_item.modified,
                            &target_file,
                            &c_opts,
                            "cp",
                        )
                        .await
                    }
                    (
                        SyncOrigin::S3 {
                            bucket: src_bucket,
                            key: src_k,
                        },
                        StorageLocation::S3 {
                            bucket: dst_bucket,
                            key: dst_prefix,
                        },
                    ) => {
                        let dst_k = compute_destination_key(dst_prefix, &rel_key, false, true);
                        eng.copy_single_s3_object(
                            src_bucket,
                            src_k,
                            src_item.size,
                            src_item.modified,
                            dst_bucket,
                            &dst_k,
                            &c_opts,
                            "cp",
                        )
                        .await
                    }
                    (SyncOrigin::Local(abs_path), StorageLocation::Local { path: dst_root }) => {
                        let target = safe_join(dst_root, &rel_key)?;
                        if !eng.output.dry_run {
                            eng.dir_cache.ensure_parent(&target)?;
                            fs::copy(abs_path, &target)?;
                        }
                        eng.output.stats.record_transfer(src_item.size, 1);
                        eng.output.emit_action(
                            "cp",
                            &abs_path.display().to_string(),
                            Some(&target.display().to_string()),
                            Some(src_item.size),
                        );
                        Ok(())
                    }
                }
            });
        }

        if opts.delete {
            let mut s3_delete_batch: Vec<(String, Option<String>)> = Vec::new();
            let mut dst_s3_bucket = String::new();

            for (rel_key, dst_item) in dst_map {
                if seen_rel_keys.contains_key(&rel_key) {
                    continue;
                }
                match dst_item.origin {
                    SyncOrigin::S3 { bucket, key } => {
                        dst_s3_bucket = bucket;
                        s3_delete_batch.push((key, None));
                        if s3_delete_batch.len() >= 1000 {
                            let chunk =
                                std::mem::replace(&mut s3_delete_batch, Vec::with_capacity(1000));
                            self.spawn_delete_batch(&mut set, &dst_s3_bucket, chunk)
                                .await?;
                        }
                    }
                    SyncOrigin::Local(abs_path) => {
                        if !self.output.dry_run {
                            let _ = fs::remove_file(&abs_path);
                        }
                        self.output.stats.record_deleted(1);
                        self.output
                            .emit_action("rm", &abs_path.display().to_string(), None, None);
                    }
                }
            }

            if !s3_delete_batch.is_empty() {
                self.spawn_delete_batch(&mut set, &dst_s3_bucket, s3_delete_batch)
                    .await?;
            }
        }

        collect_join_set(&mut set).await
    }

    async fn collect_sync_entries(
        &self,
        loc: &StorageLocation,
        filter: &GlobFilter,
        follow_symlinks: bool,
    ) -> Result<HashMap<String, SyncEntry>> {
        let mut map = HashMap::new();
        match loc {
            StorageLocation::Local { path } => {
                if !path.exists() {
                    return Ok(map);
                }
                for entry in walk_local_files(path, follow_symlinks)? {
                    let full_norm = entry.abs_path.to_string_lossy();
                    if filter.matches(&full_norm, &entry.rel_path) {
                        map.insert(
                            entry.rel_path.clone(),
                            SyncEntry {
                                size: entry.size,
                                modified: entry.modified,
                                origin: SyncOrigin::Local(entry.abs_path),
                            },
                        );
                    }
                }
            }
            StorageLocation::S3 { bucket, key } => {
                let sp = split_wildcard(key, false);
                let root_prefix = if sp.has_glob {
                    sp.root_prefix.clone()
                } else if key.is_empty() || key.ends_with('/') {
                    key.clone()
                } else {
                    format!("{key}/")
                };

                let (tx, mut rx) = mpsc::channel(1024);
                let client = self.s3.clone();
                let b_clone = bucket.clone();
                let p_clone = root_prefix.clone();
                let st = Some(self.output.stats.clone());

                tokio::spawn(async move {
                    stream_matching_s3_objects(
                        client,
                        b_clone,
                        p_clone,
                        true,
                        false,
                        Vec::new(),
                        Vec::new(),
                        st,
                        tx,
                    )
                    .await;
                });

                while let Some(res) = rx.recv().await {
                    let matched: MatchedS3Object = res?;
                    let rel = compute_rel_key(&matched.info.key, &root_prefix);
                    if filter.matches(&matched.info.key, &rel) {
                        map.insert(
                            rel,
                            SyncEntry {
                                size: matched.info.size,
                                modified: matched.info.last_modified,
                                origin: SyncOrigin::S3 {
                                    bucket: bucket.clone(),
                                    key: matched.info.key,
                                },
                            },
                        );
                    }
                }
            }
        }
        Ok(map)
    }
}

#[derive(Debug, Clone)]
struct SyncEntry {
    size: u64,
    modified: Option<DateTime<Utc>>,
    origin: SyncOrigin,
}

#[derive(Debug, Clone)]
enum SyncOrigin {
    Local(PathBuf),
    S3 { bucket: String, key: String },
}

fn should_sync_item(src: &SyncEntry, dst: &SyncEntry, opts: &SyncOptions) -> bool {
    if src.size != dst.size {
        return true;
    }
    if opts.size_only {
        return false;
    }
    if let (Some(sm), Some(dm)) = (src.modified, dst.modified) {
        if opts.exact_timestamps {
            return sm.timestamp() != dm.timestamp();
        }
        return sm > dm;
    }
    false
}

async fn collect_join_set(set: &mut JoinSet<Result<()>>) -> Result<()> {
    let mut first_err = None;
    while let Some(res) = set.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(join_err) => {
                if first_err.is_none() {
                    first_err = Some(anyhow!("Task join error: {join_err}"));
                }
            }
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    Ok(())
}

pub fn parse_duration_secs(s: &str) -> Result<u64> {
    let trimmed = s.trim();
    if let Ok(sec) = trimmed.parse::<u64>() {
        return Ok(sec);
    }
    if let Some(h) = trimmed.strip_suffix('h') {
        return Ok(h.parse::<u64>()? * 3600);
    }
    if let Some(m) = trimmed.strip_suffix('m') {
        return Ok(m.parse::<u64>()? * 60);
    }
    if let Some(sec) = trimmed.strip_suffix('s') {
        return Ok(sec.parse::<u64>()?);
    }
    bail!("Invalid duration '{s}' (examples: 3600, 1h, 30m, 90s)")
}
