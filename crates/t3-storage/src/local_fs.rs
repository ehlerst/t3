use chrono::{DateTime, Utc};
use dashmap::DashSet;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
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
        dir_cache.ensure_parent(path)?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| StorageError::Io {
                path: path.display().to_string(),
                source: e,
            })?;
        if total_size > 0 {
            file.set_len(total_size).map_err(|e| StorageError::Io {
                path: path.display().to_string(),
                source: e,
            })?;
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
