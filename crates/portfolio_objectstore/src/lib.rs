use std::path::Component;
use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use hyper::body::Body;
use once_cell::sync::Lazy;
use regex::Regex;

mod config;
pub use config::Config;
mod errors;
pub use errors::{Error, KeyError, Result};
pub(crate) mod s3;
pub use s3::S3Config;
pub use s3::S3;

pub struct Chunk {
    pub e_tag: Option<String>,
    pub chunk_number: i32,
}

/// A wrapper around [`std::path::PathBuf`] that rejects unsavory key names.
///
/// The following rules applied during the [`TryFrom<PathBuf>`] implementation:
///
/// * paths must not start with `/`
/// * paths are delimited by `/`
/// * paths are normalized (`//` are replaced with `/` and never end in `/`)
/// * paths must not contain relative segments (ie `.` or `..`)
/// * only characters explicitly documented as safe [in the S3
///   docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html) are allowed in
///   path segments
///
/// Users are allowed to break these rules at their own risk by using the less restrictive
/// [`Key.from_pathbuf()`] method.
pub struct Key {
    key: PathBuf,
}

impl Key {
    /// For users who know the keys they will be passing to [`ObjectStore`] methods are safe for
    /// their intended backend.
    ///
    /// This method skips all validation checks and so is less computationally costly but also may
    /// result in backend API errors. To signify to consumers of this library that the value may
    /// possibly be bad even though no checks are performed here, this method returns a
    /// [`std::result::Result`] that happens to always be [`std::result::Result::Ok<Key>`].
    pub fn from_pathbuf(key: PathBuf) -> Result<Key> {
        Ok(Key { key })
    }
}

impl From<&uuid::Uuid> for Key {
    fn from(uuid: &uuid::Uuid) -> Key {
        Key {
            key: PathBuf::from(uuid.to_string()),
        }
    }
}

impl From<&Key> for String {
    fn from(k: &Key) -> String {
        format!("{}", k.key.display())
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.key.display())
    }
}

impl TryFrom<PathBuf> for Key {
    type Error = Error;

    fn try_from(pb: PathBuf) -> Result<Key> {
        let key = pb
            .components()
            .try_fold(PathBuf::new(), validate_component)?;
        Ok(Key { key })
    }
}

fn validate_component(mut pb: PathBuf, c: Component<'_>) -> std::result::Result<PathBuf, KeyError> {
    static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[a-zA-Z0-9_-!.*'()]+").unwrap());
    match c {
        Component::Prefix(_) => return Err(KeyError::PrefixNotAllowed),
        Component::RootDir => return Err(KeyError::RootDirNotAllowed),
        Component::CurDir => return Err(KeyError::CurDirNotAllowed),
        Component::ParentDir => return Err(KeyError::ParentDirNotAllowed),
        Component::Normal(s) => {
            if let Some(s) = s.to_str() {
                if !RE.is_match(s) {
                    return Err(KeyError::PathComponentsMustMatchRegex(
                        RE.as_str().to_string(),
                    ));
                }
            } else {
                return Err(KeyError::PathComponentsMustBeValidUnicode);
            }
        }
    }
    pb.push(c);
    Ok(pb)
}

pub type ObjectBody = BoxStream<'static, Result<Bytes>>;

/// Provides a common interface for interacting with different kinds of backend object stores.
///
/// Object retrieval methods return [`futures::stream::Stream`] over [`bytes::Bytes`] and object
/// upload methods take (for now) [`hyper::body::Body`].
///
/// This is definitely an unstable API and may change as more backends are implemented and
/// different use cases come to light.
#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    async fn get(&self, key: &Key) -> Result<ObjectBody>;

    async fn exists(&self, key: &Key) -> Result<bool>;

    async fn put(&self, key: &Key, body: Body, content_length: u64) -> Result<()>;

    async fn delete(&self, key: &Key) -> Result<()>;

    async fn initiate_chunked_upload(&self, session_key: &Key) -> Result<String>;

    async fn upload_chunk(
        &self,
        upload_id: &str,
        session_key: &Key,
        chunk_number: i32,
        content_length: u64,
        body: Body,
    ) -> Result<Chunk>;

    async fn finalize_chunked_upload(
        &self,
        upload_id: &str,
        session_key: &Key,
        chunks: Vec<Chunk>,
        key: &Key,
    ) -> Result<()>;

    async fn abort_chunked_upload(&self, upload_id: &str, session_key: &Key) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // validate object safety
    struct Whatever {
        objectstore: Box<dyn ObjectStore>,
    }
}
