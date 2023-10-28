use axum::response::{IntoResponse, Response};
use http::StatusCode;
use serde::Serialize;
use thiserror;

use oci_spec::distribution::ErrorCode as DistributionErrorCode;
use oci_spec::distribution::ErrorInfoBuilder;
use oci_spec::distribution::ErrorResponseBuilder;
use portfolio_core::BlobError;
use portfolio_core::Error as CoreError;
use portfolio_core::ManifestError;
use portfolio_core::PortfolioErrorCode;
use portfolio_core::RepositoryError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("http invalid header name")]
    HTTPInvalidHeaderName(#[from] http::header::InvalidHeaderName),
    #[error("http invalid header value")]
    HTTPInvalidHeaderValue(#[from] http::header::InvalidHeaderValue),

    #[error("missing query parameter: {0}")]
    MissingQueryParameter(&'static str),
    #[error("missing header: {0}")]
    MissingHeader(&'static str),
    #[error("missing path parameter: {0}")]
    MissingPathParameter(&'static str),

    #[error("portfolio spec error")]
    PortfolioSpecError(PortfolioErrorCode),

    #[error("portfolio error: {0}")]
    PortfolioCoreError(#[from] CoreError),

    #[error(transparent)]
    BlobError(#[from] BlobError),

    #[error(transparent)]
    ManifestError(#[from] ManifestError),

    #[error(transparent)]
    RepositoryError(#[from] RepositoryError),

    #[error("internal server error")]
    InternalServerError(String),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::BlobError(e) => blob_error_to_response(e),
            Error::ManifestError(e) => manifest_error_to_response(e),
            Error::RepositoryError(e) => repository_error_to_response(e),
            Error::PortfolioCoreError(e) => core_error_to_response(e),
            Error::PortfolioSpecError(c) => into_nonstandard_error_response(c, None),
            Error::MissingHeader(_) => {
                (StatusCode::BAD_REQUEST, format!("{}", self)).into_response()
            }
            Error::MissingQueryParameter(_) => {
                (StatusCode::BAD_REQUEST, format!("{}", self)).into_response()
            }
            Error::MissingPathParameter(_) => {
                (StatusCode::BAD_REQUEST, format!("{}", self)).into_response()
            }
            Error::HTTPInvalidHeaderName(_) => {
                (StatusCode::BAD_REQUEST, format!("{}", self)).into_response()
            }
            Error::HTTPInvalidHeaderValue(_) => {
                (StatusCode::BAD_REQUEST, format!("{}", self)).into_response()
            }
            Error::InternalServerError(s) => {
                tracing::warn!("{:?}", s);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    String::from("internal server error"),
                )
                    .into_response()
            }
        }
    }
}

#[inline]
fn into_error_response(code: DistributionErrorCode, msg: Option<String>) -> Response {
    let msg = msg.unwrap_or(default_message(&code).to_string());
    let status_code = status_code(&code);
    let info = ErrorInfoBuilder::default()
        .code(code)
        .message(msg)
        .build()
        .expect("all required ErrorInfo fields must be initialized");

    let error_response = ErrorResponseBuilder::default()
        .errors(vec![info])
        .build()
        .expect("all required ErrorResponse fields must be initialized");

    (status_code, axum::Json(error_response)).into_response()
}

#[inline]
fn core_error_to_response(e: CoreError) -> Response {
    match e {
        CoreError::InvalidDigest(s) => {
            into_error_response(DistributionErrorCode::DigestInvalid, Some(s))
        }
        CoreError::UnsupportedDigestAlgorithm(s) => {
            into_error_response(DistributionErrorCode::DigestInvalid, Some(s))
        }
        CoreError::BackendError(s) => {
            tracing::warn!("{:?}", s);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                String::from("internal server error"),
            )
                .into_response()
        }
        CoreError::TooManyRequests => {
            into_error_response(DistributionErrorCode::TooManyRequests, None)
        }
        CoreError::OperationUnsupported => {
            into_error_response(DistributionErrorCode::Unsupported, None)
        }
        CoreError::PortfolioSpecError(c) => into_nonstandard_error_response(c, None),
    }
}

#[inline]
fn blob_error_to_response(e: BlobError) -> Response {
    match e {
        BlobError::DigestInvalid(s) => {
            into_error_response(DistributionErrorCode::DigestInvalid, Some(s))
        }
        BlobError::UuidError(e) => into_error_response(
            DistributionErrorCode::BlobUploadInvalid,
            Some(format!("{}", e)),
        ),
        BlobError::SizeInvalid => {
            into_error_response(DistributionErrorCode::SizeInvalid, None)
        }
        BlobError::BlobUploadInvalid => {
            into_error_response(DistributionErrorCode::BlobUploadInvalid, None)
        }
        BlobError::BlobUploadInvalidS(s) => {
            into_error_response(DistributionErrorCode::BlobUploadInvalid, Some(s))
        }
        BlobError::BlobUploadUnknown => {
            into_error_response(DistributionErrorCode::BlobUploadUnknown, None)
        }
        BlobError::BlobUnknown => into_error_response(DistributionErrorCode::BlobUnknown, None),
        BlobError::GenericSpecError(err) => core_error_to_response(err),
    }
}

#[inline]
fn manifest_error_to_response(e: ManifestError) -> Response {
    match e {
        ManifestError::Invalid => into_error_response(DistributionErrorCode::ManifestInvalid, None),
        ManifestError::Unknown => into_error_response(DistributionErrorCode::ManifestUnknown, None),
        ManifestError::TooBig => into_error_response(DistributionErrorCode::SizeInvalid, None),
        ManifestError::ManifestBlobUnknown => {
            into_error_response(DistributionErrorCode::ManifestBlobUnknown, None)
        }
        ManifestError::InvalidS(s) => {
            into_error_response(DistributionErrorCode::ManifestInvalid, Some(s))
        }
        ManifestError::UnknownS(s) => {
            into_error_response(DistributionErrorCode::ManifestUnknown, Some(s))
        }
        ManifestError::LayerUnknown(s) => {
            into_error_response(DistributionErrorCode::BlobUnknown, Some(s))
        }
        ManifestError::ReferencedManifestUnknown(s) => {
            into_error_response(DistributionErrorCode::ManifestUnknown, Some(s))
        }
        ManifestError::GenericSpecError(err) => core_error_to_response(err),
    }
}

#[inline]
fn repository_error_to_response(e: RepositoryError) -> Response {
    match e {
        RepositoryError::Unknown => into_error_response(DistributionErrorCode::NameUnknown, None),
        RepositoryError::Invalid => into_error_response(DistributionErrorCode::NameInvalid, None),
        RepositoryError::Unauthorized => {
            into_error_response(DistributionErrorCode::Unauthorized, None)
        }
        RepositoryError::Denied => into_error_response(DistributionErrorCode::Denied, None),
        RepositoryError::GenericSpecError(err) => core_error_to_response(err),
    }
}

#[derive(Debug, Serialize)]
// Describes a server error returned from a registry.
struct NonStandardErrorInfo {
    // The code field MUST be a unique identifier, containing only uppercase alphabetic
    // characters and underscores.
    code: PortfolioErrorCode,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    // The message field is OPTIONAL, and if present, it SHOULD be a human readable string or
    // MAY be empty.
    message: Option<String>,
}

#[derive(Debug, Serialize)]
struct NonStandardErrorResponse {
    errors: Vec<NonStandardErrorInfo>,
}

#[inline]
fn into_nonstandard_error_response(code: PortfolioErrorCode, msg: Option<String>) -> Response {
    let msg = msg.or(Some(nonstandard_default_message(&code).to_string()));
    let status_code = nonstandard_status_code(&code);
    let response = NonStandardErrorResponse {
        errors: vec![NonStandardErrorInfo { code, message: msg }],
    };
    (status_code, axum::Json(response)).into_response()
}

fn nonstandard_default_message(c: &PortfolioErrorCode) -> &str {
    match c {
        PortfolioErrorCode::ContentReferenced => "content referenced",
    }
}

fn nonstandard_status_code(c: &PortfolioErrorCode) -> StatusCode {
    match c {
        PortfolioErrorCode::ContentReferenced => StatusCode::CONFLICT,
    }
}

#[inline]
fn status_code(c: &DistributionErrorCode) -> StatusCode {
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

#[inline]
fn default_message(c: &DistributionErrorCode) -> &str {
    match c {
        DistributionErrorCode::BlobUnknown => "blob unknown to registry",
        DistributionErrorCode::BlobUploadInvalid => "blob upload invalid",
        DistributionErrorCode::BlobUploadUnknown => "blob upload unknown to registry",
        DistributionErrorCode::DigestInvalid => "provided digest did not match uploaded content",
        DistributionErrorCode::ManifestBlobUnknown => {
            "manifest references a manifest or blob unknown to registry"
        }
        DistributionErrorCode::ManifestInvalid => "manifest invalid",
        DistributionErrorCode::ManifestUnknown => "manifest unknown to registry",
        DistributionErrorCode::NameInvalid => "invalid repository name",
        DistributionErrorCode::NameUnknown => "repository name not known to registry",
        DistributionErrorCode::SizeInvalid => "provided length did not match content length",
        DistributionErrorCode::Unauthorized => "authentication required",
        DistributionErrorCode::Denied => "requested acces to the resource is denied",
        DistributionErrorCode::Unsupported => "the operation is unsupported",
        DistributionErrorCode::TooManyRequests => "too many requests",
    }
}
