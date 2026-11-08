#[cfg(not(unix))]
compile_error!("t3 only supports *nix operating systems (Linux, macOS, BSD).");

pub mod buffer_pool;
pub mod local_fs;

pub use buffer_pool::{BufferPool, PooledBuffer};
pub use local_fs::{
    read_file_bytes, safe_join, sanitize_relative_path, walk_local_files, DirCache, LocalFileEntry,
    PositionalReader, PreallocatedWriter, StorageError,
};
