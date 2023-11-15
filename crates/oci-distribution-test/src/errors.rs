use thiserror;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    LayerBuilderError(String),

    #[error("{0}")]
    ImageBuilderError(String),

    #[error("{0}")]
    IndexBuilderError(String),

    #[error("{0}")]
    CoreError(#[from] portfolio_core::Error),

    #[error("{0}")]
    TokioJoinError(#[from] tokio::task::JoinError),

    #[error("repository not found")]
    RepositoryNotFound,

    #[error("manifest {0} not found")]
    ManifestNotFound(String),

    #[error("blob {0} not found")]
    BlobNotFound(String),

    #[error("failed to collect bytes from stream: {0}")]
    StreamCollectFailed(String),

    #[error("{0}")]
    SerdeJsonToValueError(#[from] serde_json::Error),

    #[error("pushed image not found in repository: {0:?}")]
    PushedImageNotInPulledImages(portfolio_core::registry::ManifestRef),

    #[error("pushed index not found in repository: {0:?}")]
    PushedIndexNotInPulledIndices(portfolio_core::registry::ManifestRef),
}
