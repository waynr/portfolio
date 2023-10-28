use thiserror;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("portfolio error: {0}")]
    PortfolioCoreError(#[from] portfolio_core::Error),

    #[error("objectstore error: {0}")]
    ObjectStoreError(#[from] portfolio_objectstore::Error),

    #[error("sqlx error")]
    SQLXError(#[from] sqlx::Error),
    #[error("sqlx migration error")]
    SQLXMigrateError(#[from] sqlx::migrate::MigrateError),
    #[error("sea-query error")]
    SeaQueryError(#[from] sea_query::error::Error),

    #[error("http error")]
    HTTPError(#[from] http::Error),

    #[error("{0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

    #[error("failed to initiate chunked upload: {0}")]
    ObjectsFailedToInitiateChunkedUpload(&'static str),
    #[error("missing upload id for session: {0}")]
    ObjectsMissingUploadID(uuid::Uuid),

    #[error("OCI spec error: {0}")]
    OciSpecError(#[from] oci_spec::OciSpecError),

    #[error("error serializing to value")]
    SerdeJsonToValueError(#[from] serde_json::Error),

    #[error("missing query parameter: {0}")]
    MissingQueryParameter(&'static str),

    // metadata errors
    #[error("PostgresMetadataTx already rolled back or committed")]
    PostgresMetadataTxInactive,

    #[error("portfolio spec error")]
    PortfolioSpecError(portfolio_core::errors::PortfolioErrorCode),

    #[error(transparent)]
    BlobError(#[from] portfolio_core::BlobError),

    #[error(transparent)]
    ManifestError(#[from] portfolio_core::ManifestError),

    #[error(transparent)]
    RepositoryError(#[from] portfolio_core::RepositoryError),
}

impl From<Error> for portfolio_core::errors::Error {
    fn from(e: Error) -> Self {
        portfolio_core::errors::Error::BackendError(format!("{}", e))
    }
}

impl From<Error> for portfolio_core::errors::BlobError {
    fn from(e: Error) -> Self {
        match e {
            Error::BlobError(e) => e,
            _ => portfolio_core::errors::BlobError::GenericSpecError(e.into()),
        }
    }
}

impl From<Error> for portfolio_core::errors::ManifestError {
    fn from(e: Error) -> Self {
        match e {
            Error::ManifestError(e) => e,
            _ => portfolio_core::errors::ManifestError::GenericSpecError(e.into()),
        }
    }
}

impl From<Error> for portfolio_core::errors::RepositoryError {
    fn from(e: Error) -> Self {
        match e {
            Error::RepositoryError(e) => e,
            _ => portfolio_core::errors::RepositoryError::GenericSpecError(e.into()),
        }
    }
}
