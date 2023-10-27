pub use oci_spec::distribution::ErrorCode as DistributionErrorCode;
use thiserror;

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

    #[error("distribution spec error")]
    PortfolioSpecError(PortfolioErrorCode),
}

#[derive(Debug)]
pub enum PortfolioErrorCode {
    ContentReferenced = 99, // content referenced elsewhere
}
