use axum::response::{IntoResponse, Response};
use http::StatusCode;
use thiserror;

use portfolio_core::DistributionErrorCode;
use portfolio_core::status_code;

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

    // distribution error codes
    // https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes
    #[error("distribution spec error")]
    DistributionSpecError(DistributionErrorCode),

    #[error("portfolio error: {source}")]
    PortfolioCoreError { source: portfolio_core::Error },
}

impl From<portfolio_core::Error> for Error {
    fn from(e: portfolio_core::Error) -> Self {
        match e {
            portfolio_core::Error::DistributionSpecError(c) => Error::DistributionSpecError(c),
            _ => Error::PortfolioCoreError { source: e },
        }
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
