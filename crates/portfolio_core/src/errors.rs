pub use oci_spec::distribution::ErrorCode as DistributionErrorCode;
use thiserror;
use serde::Serialize;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid digest: {0}")]
    InvalidDigest(String),
    #[error("unsupported digest algorithm: {0}")]
    UnsupportedDigestAlgorithm(String),

    #[error("backend error: {0}")]
    BackendError(String),

    #[error("distribution spec error")]
    PortfolioSpecError(PortfolioErrorCode),

    #[error("digest invalid: {0}")]
    UuidError(#[from] uuid::Error),

    // Distribution Errors
    #[error("blob unknown")]
    BlobUnknown(Option<String>),
    #[error("blob upload invalid")]
    BlobUploadInvalid(Option<String>),
    #[error("blob upload unknown")]
    BlobUploadUnknown(Option<String>),
    #[error("digest invalid")]
    DigestInvalid(Option<String>),
    #[error("manifest blob unknown")]
    ManifestBlobUnknown(Option<String>),
    #[error("manifest invalid")]
    ManifestInvalid(Option<String>),
    #[error("manifest unknown")]
    ManifestUnknown(Option<String>),
    #[error("name invalid")]
    NameInvalid(Option<String>),
    #[error("name unknown")]
    NameUnknown(Option<String>),
    #[error("size invalid")]
    SizeInvalid(Option<String>),
    #[error("unauthorized")]
    Unauthorized(Option<String>),
    #[error("denied")]
    Denied(Option<String>),
    #[error("unsupported")]
    Unsupported(Option<String>),
    #[error("too many requests")]
    TooManyRequests(Option<String>),
}

#[derive(Debug, Serialize)]
pub enum PortfolioErrorCode {
    ContentReferenced = 99, // content referenced elsewhere
}
