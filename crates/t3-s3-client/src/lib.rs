pub mod auth;
pub mod client;
pub mod sigv4;
pub mod types;
pub mod xml;

pub use auth::{AwsCredentials, ConfigOverrides, ResolvedConfig};
pub use client::S3Client;
pub use types::{
    BucketInfo, CompletedPartInfo, DeleteObjectsOutput, ListObjectsPage, ObjectHead, ObjectInfo,
    PutObjectOptions, S3ClientError,
};
