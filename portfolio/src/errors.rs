use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
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

    #[error("postgres + s3 backend error: {0}")]
    PostgresS3BackendError(PostgresS3Error),

    // distribution error codes
    // https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes
    #[error("distribution spec error")]
    DistributionSpecError(DistributionErrorCode),
}

use crate::registry::impls::postgres_s3::errors::Error as PostgresS3Error;

impl From<PostgresS3Error> for Error {
    fn from(e: PostgresS3Error) -> Self {
        match e {
            PostgresS3Error::DistributionSpecError(c) => Error::DistributionSpecError(c),
            _ => Error::PostgresS3BackendError(e),
        }
    }
}

// TODO: need to generate JSON error body format as described in https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes
#[derive(Debug)]
pub enum DistributionErrorCode {
    BlobUnknown = 1,         // blob unknown to registry
    BlobUploadInvalid = 2,   // blob upload invalid
    BlobUploadUnknown = 3,   // blob upload unknown to registry
    DigestInvalid = 4,       // provided digest did not match uploaded content
    ManifestBlobUnknown = 5, // manifest references a manifest or blob unknown to registry
    ManifestInvalid = 6,     // manifest invalid
    ManifestUnknown = 7,     // manifest unknown to registry
    NameInvalid = 8,         // invalid repository name
    NameUnknown = 9,         // repository name not known to registry
    SizeInvalid = 10,        // provided length did not match content length
    Unauthorized = 12,       // authentication required
    Denied = 13,             // request access to the resource is denied
    Unsupported = 14,        // the operation is unsupported
    TooManyRequests = 15,    // too many requests
    ContentReferenced = 99,  // content referenced elsewhere
}

impl DistributionErrorCode {
    fn status_code(&self) -> StatusCode {
        match self {
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
            DistributionErrorCode::ContentReferenced => StatusCode::CONFLICT,
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::DistributionSpecError(dec) => (dec.status_code(), format!("{:?}", dec)),
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
