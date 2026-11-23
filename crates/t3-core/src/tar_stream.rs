use crate::engine::{CopyOptions, T3Engine};
use crate::filter::GlobFilter;
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use std::fs::{self, File};
use std::io::{Cursor, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use t3_s3_client::CompletedPartInfo;
use t3_storage::{safe_join, sanitize_relative_path, walk_local_files};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TarCompression {
    #[default]
    Auto,
    None,
    Lz4,
    Zstd,
    Gzip,
}

impl TarCompression {
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "auto" | "" => Ok(Self::Auto),
            "none" | "tar" | "raw" | "uncompressed" => Ok(Self::None),
            "lz4" => Ok(Self::Lz4),
            "zstd" | "zst" => Ok(Self::Zstd),
            "gzip" | "gz" | "tgz" => Ok(Self::Gzip),
            other => bail!(
                "Unsupported compression codec '{other}' (supported: auto, lz4, zstd, gzip, none)"
            ),
        }
    }

    pub fn resolve_for_key(self, s3_key: &str) -> Self {
        if self != Self::Auto {
            return self;
        }
        let lower = s3_key.to_ascii_lowercase();
        if lower.ends_with(".tar.lz4") || lower.ends_with(".tlz4") || lower.ends_with(".lz4") {
            Self::Lz4
        } else if lower.ends_with(".tar.zst") || lower.ends_with(".tzst") || lower.ends_with(".zst")
        {
            Self::Zstd
        } else if lower.ends_with(".tar.gz") || lower.ends_with(".tgz") || lower.ends_with(".gz") {
            Self::Gzip
        } else if lower.ends_with(".tar") {
            Self::None
        } else {
            // Default to lz4 when no extension is specified (1,298 MiB/s save, 1,730 MiB/s restore)
            Self::Lz4
        }
    }

    pub fn detect_from_magic(data: &[u8], fallback_key: &str) -> Self {
        if data.len() >= 4 && data[..4] == [0x04, 0x22, 0x4D, 0x18] {
            return Self::Lz4;
        }
        if data.len() >= 4 && data[..4] == [0x28, 0xB5, 0x2F, 0xFD] {
            return Self::Zstd;
        }
        if data.len() >= 2 && data[..2] == [0x1F, 0x8B] {
            return Self::Gzip;
        }
        // Standard POSIX ustar magic at offset 257 ("ustar")
        if data.len() >= 262 && &data[257..262] == b"ustar" {
            return Self::None;
        }
        let by_key = Self::Auto.resolve_for_key(fallback_key);
        if by_key == Self::Lz4 && !fallback_key.to_ascii_lowercase().contains("lz4") {
            Self::None
        } else {
            by_key
        }
    }

    pub fn content_type(self) -> &'static str {
        match self {
            Self::Lz4 => "application/x-lz4",
            Self::Zstd => "application/zstd",
            Self::Gzip => "application/gzip",
            Self::None | Self::Auto => "application/x-tar",
        }
    }
}

struct ChannelChunkWriter {
    part_size: usize,
    buf: Vec<u8>,
    tx: mpsc::Sender<Bytes>,
}

impl ChannelChunkWriter {
    fn new(part_size: usize, tx: mpsc::Sender<Bytes>) -> Self {
        Self {
            part_size,
            buf: Vec::with_capacity(part_size.min(16 * 1024 * 1024)),
            tx,
        }
    }

    fn finish(mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            let chunk = Bytes::from(std::mem::take(&mut self.buf));
            self.tx.blocking_send(chunk).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "upload closed")
            })?;
        }
        Ok(())
    }
}

impl Write for ChannelChunkWriter {
    fn write(&mut self, mut data: &[u8]) -> std::io::Result<usize> {
        let total = data.len();
        while !data.is_empty() {
            let rem = self.part_size - self.buf.len();
            let take = data.len().min(rem);
            self.buf.extend_from_slice(&data[..take]);
            data = &data[take..];

            if self.buf.len() >= self.part_size {
                let next_cap = self.part_size.min(16 * 1024 * 1024);
                let chunk = Bytes::from(std::mem::replace(
                    &mut self.buf,
                    Vec::with_capacity(next_cap),
                ));
                self.tx.blocking_send(chunk).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "upload closed")
                })?;
            }
        }
        Ok(total)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub async fn tar_upload_dir_to_s3(
    engine: &T3Engine,
    src_dir: &Path,
    dst_bucket: &str,
    dst_key: &str,
    opts: &CopyOptions,
) -> Result<()> {
    if !src_dir.exists() {
        return Err(t3_s3_client::S3ClientError::NotFound(format!(
            "Local path not found: {}",
            src_dir.display()
        ))
        .into());
    }

    let filter = GlobFilter::compile(None, &opts.includes, &opts.excludes)?;
    let all_entries = walk_local_files(src_dir, !opts.no_follow_symlinks)?;
    let mut matched_entries = Vec::with_capacity(all_entries.len());
    let mut total_raw_bytes = 0u64;

    for entry in all_entries {
        let full_norm = entry.abs_path.to_string_lossy();
        if filter.matches(&full_norm, &entry.rel_path) {
            total_raw_bytes += entry.size;
            matched_entries.push(entry);
        }
    }
    matched_entries.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));

    let src_disp = src_dir.display().to_string();
    let dst_disp = format!("s3://{dst_bucket}/{dst_key}");

    if engine.output.dry_run {
        engine.output.emit_action(
            "tar-sync",
            &src_disp,
            Some(&dst_disp),
            Some(total_raw_bytes),
        );
        return Ok(());
    }

    let codec = opts.compression.resolve_for_key(dst_key);
    let part_size = opts
        .effective_part_size(total_raw_bytes)
        .max(5 * 1024 * 1024) as usize;
    let concurrency = opts.concurrency.max(1);
    let (tx, mut rx) = mpsc::channel::<Bytes>(concurrency * 2);

    let producer_handle = tokio::task::spawn_blocking(move || -> Result<u64> {
        let chunk_writer = ChannelChunkWriter::new(part_size, tx);
        let files_count = matched_entries.len() as u64;

        let write_entries = |w: &mut dyn Write| -> Result<()> {
            let mut builder = tar::Builder::new(w);
            builder.follow_symlinks(false);
            for entry in &matched_entries {
                let clean_rel = sanitize_relative_path(&entry.rel_path)?;
                let mut f = File::open(&entry.abs_path)
                    .with_context(|| format!("Failed to open {}", entry.abs_path.display()))?;
                builder
                    .append_file(&clean_rel, &mut f)
                    .with_context(|| format!("Failed to tar {}", entry.abs_path.display()))?;
            }
            builder.finish()?;
            Ok(())
        };

        match codec {
            TarCompression::Lz4 => {
                let mut enc = lz4_flex::frame::FrameEncoder::new(chunk_writer);
                write_entries(&mut enc)?;
                let cw = enc.finish()?;
                cw.finish()?;
            }
            TarCompression::Zstd => {
                let mut enc = zstd::stream::write::Encoder::new(chunk_writer, 1)?;
                write_entries(&mut enc)?;
                let cw = enc.finish()?;
                cw.finish()?;
            }
            TarCompression::Gzip => {
                let mut enc =
                    flate2::write::GzEncoder::new(chunk_writer, flate2::Compression::fast());
                write_entries(&mut enc)?;
                let cw = enc.finish()?;
                cw.finish()?;
            }
            TarCompression::None | TarCompression::Auto => {
                let mut cw = chunk_writer;
                write_entries(&mut cw)?;
                cw.finish()?;
            }
        }
        Ok(files_count)
    });

    let mut put_opts = opts.put_opts.clone();
    if put_opts.content_type.is_none() {
        put_opts.content_type = Some(codec.content_type().to_string());
    }

    let first_chunk = rx.recv().await.unwrap_or_default();
    let second_chunk_opt = rx.recv().await;

    let mut total_compressed_bytes = first_chunk.len() as u64;

    if let Some(second_chunk) = second_chunk_opt {
        // Multi-part streaming upload
        let upload_id = engine
            .s3
            .create_multipart_upload(dst_bucket, dst_key, &put_opts)
            .await?;
        let part_sem = Arc::new(Semaphore::new(concurrency));
        let mut part_set = JoinSet::new();
        let mut part_num = 1i32;

        for initial in [first_chunk, second_chunk] {
            let pnum = part_num;
            part_num += 1;
            if pnum == 2 {
                total_compressed_bytes += initial.len() as u64;
            }
            let permit = part_sem.clone().acquire_owned().await?;
            let c = engine.s3.clone();
            let b = dst_bucket.to_string();
            let k = dst_key.to_string();
            let uid = upload_id.clone();
            part_set.spawn(async move {
                let _p = permit;
                let etag = c.upload_part(&b, &k, &uid, pnum, initial).await?;
                Ok::<CompletedPartInfo, anyhow::Error>(CompletedPartInfo {
                    part_number: pnum,
                    etag,
                })
            });
        }

        while let Some(chunk) = rx.recv().await {
            total_compressed_bytes += chunk.len() as u64;
            let pnum = part_num;
            part_num += 1;
            let permit = part_sem.clone().acquire_owned().await?;
            let c = engine.s3.clone();
            let b = dst_bucket.to_string();
            let k = dst_key.to_string();
            let uid = upload_id.clone();
            part_set.spawn(async move {
                let _p = permit;
                let etag = c.upload_part(&b, &k, &uid, pnum, chunk).await?;
                Ok::<CompletedPartInfo, anyhow::Error>(CompletedPartInfo {
                    part_number: pnum,
                    etag,
                })
            });
        }

        let mut completed = Vec::with_capacity((part_num - 1) as usize);
        while let Some(res) = part_set.join_next().await {
            match res? {
                Ok(info) => completed.push(info),
                Err(e) => {
                    let _ = engine
                        .s3
                        .abort_multipart_upload(dst_bucket, dst_key, &upload_id)
                        .await;
                    return Err(e);
                }
            }
        }

        engine
            .s3
            .complete_multipart_upload_with_opts(
                dst_bucket,
                dst_key,
                &upload_id,
                completed,
                Some(&put_opts),
            )
            .await?;
        let _files_count = producer_handle.await??;
        engine
            .output
            .stats
            .record_transfer(total_compressed_bytes, (part_num - 1) as u64);
    } else {
        // Single-PUT fast path!
        let _files_count = producer_handle.await??;
        engine
            .s3
            .put_object(dst_bucket, dst_key, first_chunk, &put_opts)
            .await?;
        engine
            .output
            .stats
            .record_transfer(total_compressed_bytes, 1);
    }

    engine.output.emit_action(
        "tar-sync",
        &src_disp,
        Some(&dst_disp),
        Some(total_compressed_bytes),
    );
    Ok(())
}

pub async fn tar_download_s3_to_dir(
    engine: &T3Engine,
    src_bucket: &str,
    src_key: &str,
    dst_dir: &Path,
    opts: &CopyOptions,
) -> Result<()> {
    let head = engine
        .s3
        .head_object(src_bucket, src_key, opts.version_id.as_deref())
        .await?;
    let size = head.content_length;

    let src_disp = format!("s3://{src_bucket}/{src_key}");
    let dst_disp = dst_dir.display().to_string();

    if engine.output.dry_run {
        engine
            .output
            .emit_action("tar-sync", &src_disp, Some(&dst_disp), Some(size));
        return Ok(());
    }

    let part_size = opts.effective_part_size(size).max(5 * 1024 * 1024);
    let (archive_bytes, num_parts) = if size <= part_size || opts.concurrency <= 1 {
        let bytes = engine
            .s3
            .get_object_bytes(src_bucket, src_key, None, opts.version_id.as_deref())
            .await?;
        (bytes, 1u64)
    } else {
        let n_parts = size.div_ceil(part_size);
        let part_sem = Arc::new(Semaphore::new(opts.concurrency.max(1)));
        let mut part_set = JoinSet::new();

        for part_idx in 0..n_parts {
            let start = part_idx * part_size;
            let end = (start + part_size - 1).min(size - 1);
            let permit = part_sem.clone().acquire_owned().await?;
            let c = engine.s3.clone();
            let b = src_bucket.to_string();
            let k = src_key.to_string();
            let vid = opts.version_id.clone();

            part_set.spawn(async move {
                let _p = permit;
                let chunk = c
                    .get_object_bytes(&b, &k, Some((start, end)), vid.as_deref())
                    .await?;
                Ok::<(u64, Bytes), anyhow::Error>((part_idx, chunk))
            });
        }

        let mut chunks = Vec::with_capacity(n_parts as usize);
        while let Some(res) = part_set.join_next().await {
            chunks.push(res??);
        }
        chunks.sort_by_key(|(idx, _)| *idx);
        let mut full = Vec::with_capacity(size as usize);
        for (_, ch) in chunks {
            full.extend_from_slice(&ch);
        }
        (Bytes::from(full), n_parts)
    };

    let codec = match opts.compression {
        TarCompression::Auto => TarCompression::detect_from_magic(&archive_bytes, src_key),
        explicit => explicit,
    };

    let dst_root = dst_dir.to_path_buf();
    let dir_cache = engine.dir_cache.clone();
    let filter = GlobFilter::compile(None, &opts.includes, &opts.excludes)?;

    tokio::task::spawn_blocking(move || -> Result<()> {
        fs::create_dir_all(&dst_root)?;
        let cursor = Cursor::new(archive_bytes);
        let reader: Box<dyn Read> = match codec {
            TarCompression::Lz4 => Box::new(lz4_flex::frame::FrameDecoder::new(cursor)),
            TarCompression::Zstd => Box::new(zstd::stream::read::Decoder::new(cursor)?),
            TarCompression::Gzip => Box::new(flate2::read::GzDecoder::new(cursor)),
            TarCompression::None | TarCompression::Auto => Box::new(cursor),
        };

        let mut archive = tar::Archive::new(reader);
        for entry_res in archive.entries()? {
            let mut entry = entry_res?;
            let raw_path = entry.path()?.to_string_lossy().into_owned();
            // Always validate path traversal (`..`) on every tar entry!
            let clean_rel = sanitize_relative_path(&raw_path)?;
            let rel_str = clean_rel.to_string_lossy();
            if !filter.matches(&rel_str, &rel_str) {
                continue;
            }
            let target_path = safe_join(&dst_root, &rel_str)?;
            let entry_type = entry.header().entry_type();
            if entry_type.is_dir() {
                fs::create_dir_all(&target_path)?;
            } else if entry_type.is_file() || entry_type.is_contiguous() {
                dir_cache.ensure_parent(&target_path)?;
                let mut out_file = File::create(&target_path)?;
                std::io::copy(&mut entry, &mut out_file)?;
                if let Ok(mode) = entry.header().mode() {
                    let _ =
                        fs::set_permissions(&target_path, fs::Permissions::from_mode(mode & 0o777));
                }
            }
        }
        Ok(())
    })
    .await??;

    engine.output.stats.record_transfer(size, num_parts);
    engine
        .output
        .emit_action("tar-sync", &src_disp, Some(&dst_disp), Some(size));
    Ok(())
}
