#[cfg(not(unix))]
compile_error!("t3 only supports *nix operating systems (Linux, macOS, BSD).");

pub mod buffer_pool;
pub mod local_fs;

pub use buffer_pool::{BufferPool, PooledBuffer};
pub use local_fs::{
    compute_adaptive_part_size, read_file_bytes, safe_join, sanitize_relative_path,
    walk_local_files, DirCache, LocalFileEntry, PartResumeMeta, PositionalReader,
    PreallocatedWriter, ResumeTracker, StorageError, DEFAULT_SINGLE_PUT_THRESHOLD, MAX_S3_PARTS,
    MIN_S3_PART_SIZE,
};
