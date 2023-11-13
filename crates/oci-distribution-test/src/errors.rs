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

    #[error("{0}")]
    SerdeJsonToValueError(#[from] serde_json::Error),
}
