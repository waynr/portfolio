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

    // distribution error codes
    // https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes
    #[error("distribution spec error")]
    DistributionSpecError(DistributionErrorCode),

    #[error("operation unsupported")]
    OperationUnsupported,

    #[error("too many requests")]
    TooManyRequests,

    #[error("distribution spec error")]
    PortfolioSpecError(PortfolioErrorCode),
}

#[derive(Debug, Serialize)]
pub enum PortfolioErrorCode {
    ContentReferenced = 99, // content referenced elsewhere
}

#[derive(thiserror::Error, Debug)]
pub enum BlobError {
    #[error("digest invalid: {0}")]
    DigestInvalid(String),

    #[error("digest invalid: {0}")]
    UuidError(#[from] uuid::Error),

    #[error("blob unknown: {0}")]
    BlobUnknown(String),

    #[error("blob upload invalid: {0}")]
    BlobUploadInvalid(String),

    #[error("blob upload unknown: {0}")]
    BlobUploadUnknown(String),

    #[error(transparent)]
    GenericSpecError(Error),
}

impl From<Error> for BlobError {
    fn from(e: Error) -> BlobError {
        match e {
            Error::InvalidDigest(s) => BlobError::DigestInvalid(s),
            Error::UnsupportedDigestAlgorithm(s) => BlobError::DigestInvalid(s),
            Error::BackendError(s) => BlobError::DigestInvalid(s),
            e => BlobError::GenericSpecError(e),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ManifestError {
    #[error("reference invalid: {0}")]
    Invalid(String),

    #[error("digest invalid: {0}")]
    Unknown(String),

    #[error(transparent)]
    GenericSpecError(Error),
}

impl From<Error> for ManifestError {
    fn from(e: Error) -> ManifestError {
        match e {
            Error::InvalidDigest(s) => ManifestError::Invalid(s),
            Error::UnsupportedDigestAlgorithm(s) => ManifestError::Invalid(s),
            _ => unreachable!(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RepositoryError {
    #[error("invalid")]
    Invalid,

    #[error("unknown")]
    Unknown,

    #[error("unauthorized")]
    Unauthorized,

    #[error("denied")]
    Denied,

    #[error(transparent)]
    GenericSpecError(Error),
}
