mod traits;
pub use traits::ObjectStore;

mod s3;
pub use s3::{S3Config, S3};
