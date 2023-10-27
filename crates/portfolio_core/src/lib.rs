//#![warn(missing_docs)]
//! # Portfolio Core
//!
//! `portfolio_core` provides basic interoperability types between [`portfolio_http`] [an OCI
//! Distribution Spec implementation](https://github.com/opencontainers/distribution-spec) and
//! backend implementations such as [`portfolio_backend_postgres`].
//!
//! The primary set of interoperability types can be found in the [`crate::registry`] module.
pub mod errors;
pub use errors::{DistributionErrorCode, PortfolioErrorCode, Error, Result, status_code};

mod oci_digest;
pub use oci_digest::{DigestState, Digester, OciDigest};

pub mod registry;

mod stream;
pub use stream::ChunkedBody;
pub use stream::DigestBody;
