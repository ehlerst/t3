use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashSet;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use walkdir::WalkDir;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("I/O error on '{path}': {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },

    #[error("Security violation: path traversal rejected for key '{0}'")]
    PathTraversal(String),
}

#[derive(Debug, Clone, Default)]
pub struct DirCache {
    created: Arc<DashSet<PathBuf>>,
}

impl DirCache {
    pub fn new() -> Self {
        Self {
            created: Arc::new(DashSet::new()),
        }
    }

    pub fn ensure_parent(&self, file_path: &Path) -> Result<(), StorageError> {
        let Some(parent) = file_path.parent() else {
            return Ok(());
        };
        if parent.as_os_str().is_empty() || self.created.contains(parent) {
            return Ok(());
        }
        fs::create_dir_all(parent).map_err(|e| StorageError::Io {
            path: parent.display().to_string(),
            source: e,
        })?;
        self.created.insert(parent.to_path_buf());
        Ok(())
    }
}

/// Validates and normalizes a relative S3 key so it can never escape a destination directory
/// via `..` (`Component::ParentDir`) or root injection (fixes s5cmd issue #872).
pub fn sanitize_relative_path(rel_key: &str) -> Result<PathBuf, StorageError> {
    let trimmed = rel_key.trim_start_matches('/');
    if trimmed.is_empty() {
        return Err(StorageError::PathTraversal(rel_key.to_string()));
    }

    let path = Path::new(trimmed);
    let mut clean = PathBuf::new();

    for comp in path.components() {
        match comp {
            Component::Normal(seg) => {
                if seg == ".." {
                    return Err(StorageError::PathTraversal(rel_key.to_string()));
                }
                clean.push(seg);
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(StorageError::PathTraversal(rel_key.to_string()));
            }
        }
    }

    if clean.as_os_str().is_empty() {
        return Err(StorageError::PathTraversal(rel_key.to_string()));
    }
    Ok(clean)
}

pub fn safe_join(base_dir: &Path, rel_key: &str) -> Result<PathBuf, StorageError> {
    let clean_rel = sanitize_relative_path(rel_key)?;
    Ok(base_dir.join(clean_rel))
}

#[derive(Clone)]
pub struct PreallocatedWriter {
    file: Arc<File>,
    path: PathBuf,
}

impl PreallocatedWriter {
    pub fn create(
        path: &Path,
        total_size: u64,
        dir_cache: &DirCache,
    ) -> Result<Self, StorageError> {
        Self::open_existing_or_create(path, total_size, dir_cache, true)
    }

    pub fn open_existing_or_create(
        path: &Path,
        total_size: u64,
        dir_cache: &DirCache,
        truncate: bool,
    ) -> Result<Self, StorageError> {
        dir_cache.ensure_parent(path)?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(truncate)
            .open(path)
            .map_err(|e| StorageError::Io {
                path: path.display().to_string(),
                source: e,
            })?;
        if total_size > 0 {
            let cur_len = file.metadata().map(|m| m.len()).unwrap_or(0);
            if truncate || cur_len != total_size {
                file.set_len(total_size).map_err(|e| StorageError::Io {
                    path: path.display().to_string(),
                    source: e,
                })?;
            }
        }
        Ok(Self {
            file: Arc::new(file),
            path: path.to_path_buf(),
        })
    }

    #[inline]
    pub fn write_at(&self, offset: u64, data: &[u8]) -> Result<(), StorageError> {
        self.file
            .write_all_at(data, offset)
            .map_err(|e| StorageError::Io {
                path: self.path.display().to_string(),
                source: e,
            })
    }
}

pub const MIN_S3_PART_SIZE: u64 = 5 * 1024 * 1024;
pub const DEFAULT_SINGLE_PUT_THRESHOLD: u64 = 16 * 1024 * 1024;
pub const MAX_S3_PARTS: u64 = 10_000;

/// Adaptive part sizing by object size:
/// - Files <= 16 MiB take the single-PUT / single-GET fast path (returns 16 MiB threshold).
/// - Larger files scale from 16 MiB -> 32 MiB -> 64 MiB -> 128+ MiB while keeping part count <= 10,000.
pub fn compute_adaptive_part_size(total_size: u64, explicit_part_size_bytes: Option<u64>) -> u64 {
    if let Some(exp) = explicit_part_size_bytes {
        let base = exp.max(MIN_S3_PART_SIZE);
        let min_for_limit = total_size
            .div_ceil(MAX_S3_PARTS - 100)
            .max(MIN_S3_PART_SIZE);
        return base.max(min_for_limit.div_ceil(1024 * 1024) * 1024 * 1024);
    }

    if total_size <= DEFAULT_SINGLE_PUT_THRESHOLD {
        return DEFAULT_SINGLE_PUT_THRESHOLD;
    }
    let tier_size = if total_size <= 128 * 1024 * 1024 {
        16 * 1024 * 1024
    } else if total_size <= 1024 * 1024 * 1024 {
        32 * 1024 * 1024
    } else if total_size <= 8 * 1024 * 1024 * 1024 {
        64 * 1024 * 1024
    } else {
        128 * 1024 * 1024
    };
    let min_for_limit = total_size
        .div_ceil(MAX_S3_PARTS - 100)
        .max(MIN_S3_PART_SIZE);
    tier_size.max(min_for_limit.div_ceil(1024 * 1024) * 1024 * 1024)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartResumeMeta {
    pub etag: Option<String>,
    pub total_size: u64,
    pub part_size: u64,
    pub completed_parts: std::collections::BTreeSet<u64>,
}

#[derive(Clone)]
pub struct ResumeTracker {
    pub part_file_path: PathBuf,
    pub meta_file_path: PathBuf,
    meta: Arc<parking_lot::Mutex<PartResumeMeta>>,
}

impl ResumeTracker {
    pub fn part_paths_for(dst_file: &Path) -> (PathBuf, PathBuf) {
        let mut part_os = dst_file.as_os_str().to_os_string();
        part_os.push(".t3part");
        let mut meta_os = dst_file.as_os_str().to_os_string();
        meta_os.push(".t3part.meta");
        (PathBuf::from(part_os), PathBuf::from(meta_os))
    }

    pub fn load_or_init(
        dst_file: &Path,
        etag: Option<String>,
        total_size: u64,
        part_size: u64,
        resume_enabled: bool,
    ) -> (Self, bool) {
        let (part_file_path, meta_file_path) = Self::part_paths_for(dst_file);
        if resume_enabled {
            if let (Ok(file_meta), Ok(meta_bytes)) =
                (fs::metadata(&part_file_path), fs::read(&meta_file_path))
            {
                if file_meta.len() == total_size {
                    if let Ok(saved) = serde_json::from_slice::<PartResumeMeta>(&meta_bytes) {
                        let etag_matches = match (&saved.etag, &etag) {
                            (Some(a), Some(b)) => a == b,
                            _ => true,
                        };
                        if saved.total_size == total_size
                            && saved.part_size == part_size
                            && etag_matches
                        {
                            return (
                                Self {
                                    part_file_path,
                                    meta_file_path,
                                    meta: Arc::new(parking_lot::Mutex::new(saved)),
                                },
                                true,
                            );
                        }
                    }
                }
            }
        }

        let fresh = PartResumeMeta {
            etag,
            total_size,
            part_size,
            completed_parts: std::collections::BTreeSet::new(),
        };
        (
            Self {
                part_file_path,
                meta_file_path,
                meta: Arc::new(parking_lot::Mutex::new(fresh)),
            },
            false,
        )
    }

    pub fn is_part_completed(&self, part_idx: u64) -> bool {
        self.meta.lock().completed_parts.contains(&part_idx)
    }

    pub fn mark_part_completed(&self, part_idx: u64) -> Result<(), StorageError> {
        let json_bytes = {
            let mut guard = self.meta.lock();
            guard.completed_parts.insert(part_idx);
            serde_json::to_vec(&*guard).unwrap_or_default()
        };
        fs::write(&self.meta_file_path, json_bytes).map_err(|e| StorageError::Io {
            path: self.meta_file_path.display().to_string(),
            source: e,
        })
    }

    pub fn finalize(&self, dst_file: &Path) -> Result<(), StorageError> {
        fs::rename(&self.part_file_path, dst_file).map_err(|e| StorageError::Io {
            path: dst_file.display().to_string(),
            source: e,
        })?;
        let _ = fs::remove_file(&self.meta_file_path);
        Ok(())
    }
}

#[derive(Clone)]
pub struct PositionalReader {
    file: Arc<File>,
    path: PathBuf,
    size: u64,
}

impl PositionalReader {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let file = File::open(path).map_err(|e| StorageError::Io {
            path: path.display().to_string(),
            source: e,
        })?;
        let meta = file.metadata().map_err(|e| StorageError::Io {
            path: path.display().to_string(),
            source: e,
        })?;
        Ok(Self {
            file: Arc::new(file),
            path: path.to_path_buf(),
            size: meta.len(),
        })
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    #[inline]
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), StorageError> {
        self.file
            .read_exact_at(buf, offset)
            .map_err(|e| StorageError::Io {
                path: self.path.display().to_string(),
                source: e,
            })
    }
}

struct MmapRegion {
    ptr: *mut libc::c_void,
    len: usize,
}

// Safety: MmapRegion holds a read-only (`PROT_READ`) memory map backed by a file descriptor
// and is immutable for its lifetime until `munmap` in `Drop`.
unsafe impl Send for MmapRegion {}
unsafe impl Sync for MmapRegion {}

impl AsRef<[u8]> for MmapRegion {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len) }
    }
}

impl Drop for MmapRegion {
    fn drop(&mut self) {
        if self.len > 0 && !self.ptr.is_null() {
            unsafe {
                let _ = libc::munmap(self.ptr, self.len);
            }
        }
    }
}

/// Reads a local file into `Bytes`, using zero-copy `mmap` (`PROT_READ, MAP_SHARED`) for files
/// >= 256 KiB to avoid anonymous heap allocation and redundant page-cache-to-userspace copies.
pub fn read_file_bytes(path: &Path, size_hint: u64) -> Result<Bytes, StorageError> {
    if size_hint == 0 {
        let data = fs::read(path).map_err(|e| StorageError::Io {
            path: path.display().to_string(),
            source: e,
        })?;
        return Ok(Bytes::from(data));
    }

    if size_hint >= 1024 * 1024 {
        let file = File::open(path).map_err(|e| StorageError::Io {
            path: path.display().to_string(),
            source: e,
        })?;
        let len = size_hint as usize;
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr != libc::MAP_FAILED && !ptr.is_null() {
            unsafe {
                let _ = libc::madvise(ptr, len, libc::MADV_SEQUENTIAL);
            }
            return Ok(Bytes::from_owner(MmapRegion { ptr, len }));
        }
    }

    let data = fs::read(path).map_err(|e| StorageError::Io {
        path: path.display().to_string(),
        source: e,
    })?;
    Ok(Bytes::from(data))
}

#[derive(Debug, Clone)]
pub struct LocalFileEntry {
    pub abs_path: PathBuf,
    pub rel_path: String,
    pub size: u64,
    pub modified: Option<DateTime<Utc>>,
}

pub fn walk_local_files(
    root: &Path,
    follow_symlinks: bool,
) -> Result<Vec<LocalFileEntry>, StorageError> {
    if root.is_file() {
        let meta = fs::metadata(root).map_err(|e| StorageError::Io {
            path: root.display().to_string(),
            source: e,
        })?;
        let modified = meta.modified().ok().map(DateTime::<Utc>::from);
        let rel_path = root
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        return Ok(vec![LocalFileEntry {
            abs_path: root.to_path_buf(),
            rel_path,
            size: meta.len(),
            modified,
        }]);
    }

    let mut entries = Vec::new();
    for entry_res in WalkDir::new(root).follow_links(follow_symlinks) {
        let entry = entry_res.map_err(|e| StorageError::Io {
            path: root.display().to_string(),
            source: e
                .into_io_error()
                .unwrap_or_else(|| std::io::Error::other("walkdir error")),
        })?;
        if entry.file_type().is_file() {
            let meta = entry.metadata().map_err(|e| StorageError::Io {
                path: entry.path().display().to_string(),
                source: e
                    .into_io_error()
                    .unwrap_or_else(|| std::io::Error::other("metadata error")),
            })?;
            let rel = entry
                .path()
                .strip_prefix(root)
                .unwrap_or(entry.path())
                .to_string_lossy()
                .into_owned();
            let modified = meta.modified().ok().map(DateTime::<Utc>::from);
            entries.push(LocalFileEntry {
                abs_path: entry.path().to_path_buf(),
                rel_path: rel,
                size: meta.len(),
                modified,
            });
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_traversal_guard() {
        assert!(sanitize_relative_path("../etc/passwd").is_err());
        assert!(sanitize_relative_path("a/b/../../etc/shadow").is_err());
        assert!(sanitize_relative_path("logs/2026/01/data.parquet").is_ok());

        let base = Path::new("/tmp/downloads");
        let joined = safe_join(base, "logs/2026/app.log").unwrap();
        assert_eq!(joined, PathBuf::from("/tmp/downloads/logs/2026/app.log"));
    }

    #[test]
    fn test_preallocated_writer_and_positional_reader() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("nested/dir/out.bin");
        let cache = DirCache::new();

        let writer = PreallocatedWriter::create(&file_path, 10, &cache).unwrap();
        writer.write_at(5, b"world").unwrap();
        writer.write_at(0, b"hello").unwrap();

        let reader = PositionalReader::open(&file_path).unwrap();
        assert_eq!(reader.size(), 10);
        let mut buf = [0u8; 10];
        reader.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"helloworld");
    }
}
