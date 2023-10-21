use thiserror;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("portfolio error: {0}")]
    PortfolioError(String),

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

    #[error("error serializing to value")]
    SerdeJsonToValueError(#[from] serde_json::Error),

    #[error("missing query parameter: {0}")]
    MissingQueryParameter(&'static str),

    // metadata errors
    #[error("PostgresMetadataTx already rolled back or committed")]
    PostgresMetadataTxInactive,

    // distribution error codes
    // https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes
    #[error("distribution spec error")]
    DistributionSpecError(portfolio_core::errors::DistributionErrorCode),
}

impl From<portfolio_core::errors::Error> for Error {
    fn from(e: portfolio_core::errors::Error) -> Error {
        Error::PortfolioError(format!("{}", e))
    }
}

impl From<Error> for portfolio_core::errors::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::DistributionSpecError(c) => portfolio_core::errors::Error::DistributionSpecError(c),
            _ => portfolio_core::errors::Error::BackendError(Box::new(e)),
        }
    }
}
