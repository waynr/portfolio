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

    #[error("size invalid")]
    SizeInvalid,

    #[error("blob unknown")]
    BlobUnknown,

    #[error("blob upload invalid")]
    BlobUploadInvalid,

    #[error("{0}")]
    BlobUploadInvalidS(String),

    #[error("blob upload unknown")]
    BlobUploadUnknown,

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
    #[error("invalid")]
    Invalid,

    #[error("unknown")]
    Unknown,

    #[error("unknown")]
    ManifestBlobUnknown,

    #[error("manifest too big")]
    TooBig,

    #[error("{0}")]
    InvalidS(String),

    #[error("{0}")]
    UnknownS(String),

    #[error("{0}")]
    LayerUnknown(String),

    #[error("{0}")]
    ReferencedManifestUnknown(String),

    #[error("{0}")]
    GenericSpecError(Error),
}

impl From<Error> for ManifestError {
    fn from(e: Error) -> ManifestError {
        match e {
            Error::InvalidDigest(s) => ManifestError::InvalidS(s),
            Error::UnsupportedDigestAlgorithm(s) => ManifestError::InvalidS(s),
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
