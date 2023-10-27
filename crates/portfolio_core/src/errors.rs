use axum::response::{IntoResponse, Response};
use http::StatusCode;
pub use oci_spec::distribution::ErrorCode as DistributionErrorCode;
use thiserror;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    AddrParseError(#[from] std::net::AddrParseError),

    #[error("config deserialization error")]
    ConfigError(#[from] serde_yaml::Error),
    #[error("io error")]
    IOError(#[from] std::io::Error),
    #[error("http error")]
    HTTPError(#[from] http::Error),
    #[error("http invalid header name")]
    HTTPInvalidHeaderName(#[from] http::header::InvalidHeaderName),
    #[error("http invalid header value")]
    HTTPInvalidHeaderValue(#[from] http::header::InvalidHeaderValue),
    #[error("{0}")]
    HyperError(#[from] hyper::Error),

    // input validation errors
    #[error("invalid uuid")]
    InvalidUuid(#[from] uuid::Error),
    #[error("invalid digest: {0}")]
    InvalidDigest(String),
    #[error("unsupported digest algorithm: {0}")]
    UnsupportedDigestAlgorithm(String),

    #[error("missing query parameter: {0}")]
    MissingQueryParameter(&'static str),
    #[error("missing header: {0}")]
    MissingHeader(&'static str),
    #[error("missing path parameter: {0}")]
    MissingPathParameter(&'static str),
    #[error("invalid header value: {0}")]
    InvalidHeaderValue(&'static str),

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

pub fn status_code(c: &DistributionErrorCode) -> StatusCode {
    match c {
        DistributionErrorCode::BlobUnknown => StatusCode::NOT_FOUND,
        DistributionErrorCode::BlobUploadInvalid => StatusCode::RANGE_NOT_SATISFIABLE,
        DistributionErrorCode::BlobUploadUnknown => StatusCode::BAD_REQUEST,
        DistributionErrorCode::DigestInvalid => StatusCode::BAD_REQUEST,
        DistributionErrorCode::ManifestBlobUnknown => StatusCode::NOT_FOUND,
        DistributionErrorCode::ManifestInvalid => StatusCode::BAD_REQUEST,
        DistributionErrorCode::ManifestUnknown => StatusCode::NOT_FOUND,
        DistributionErrorCode::NameInvalid => StatusCode::BAD_REQUEST,
        DistributionErrorCode::NameUnknown => StatusCode::NOT_FOUND,
        DistributionErrorCode::SizeInvalid => StatusCode::BAD_REQUEST,
        DistributionErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
        DistributionErrorCode::Denied => StatusCode::FORBIDDEN,
        DistributionErrorCode::Unsupported => StatusCode::NOT_IMPLEMENTED,
        DistributionErrorCode::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::DistributionSpecError(dec) => (status_code(&dec), format!("{:?}", dec)),
            Error::InvalidUuid(_) => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::InvalidDigest(_) => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::MissingHeader(_) => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::InvalidHeaderValue(_) => (StatusCode::BAD_REQUEST, format!("{}", self)),
            Error::MissingPathParameter(_) => (StatusCode::BAD_REQUEST, format!("{}", self)),
            _ => {
                tracing::warn!("{:?}", self);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    String::from("something bad happened"),
                )
            }
        }
        .into_response()
    }
}
