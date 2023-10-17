use chrono::NaiveDate;
use sea_query::Iden;
use sqlx::types::Json;
use uuid::Uuid;

use crate::errors::{Error, Result};
use crate::http::headers::ContentRange;
use crate::{DigestState, DistributionErrorCode};

#[derive(Debug, sqlx::FromRow)]
pub struct UploadSession {
    pub uuid: Uuid,
    pub start_date: NaiveDate,
    pub upload_id: Option<String>,
    pub chunk_number: i32,
    pub last_range_end: i64,
    pub digest_state: Option<Json<DigestState>>,
}

impl UploadSession {
    /// verify the request's ContentRange against the last chunk's end of range
    pub fn validate_range(&mut self, content_range: &ContentRange) -> Result<()> {
        let ContentRange { start, end: _ } = content_range;
        if *start == 0 && self.chunk_number == 1 {
            return Ok(());
        }
        if *start as i64 != self.last_range_end + 1 {
            tracing::debug!("{content_range:?} is invalid");
            return Err(Error::DistributionSpecError(
                DistributionErrorCode::BlobUploadInvalid,
            ));
        }
        Ok(())
    }
}

#[derive(Iden)]
pub enum UploadSessions {
    Table,
    Uuid,
    StartDate,
    UploadId,
    ChunkNumber,
    LastRangeEnd,
    DigestState,
}

#[derive(Default, sqlx::FromRow)]
pub struct Chunk {
    pub e_tag: Option<String>,
    pub chunk_number: i32,
}

#[derive(Iden)]
pub enum Chunks {
    Table,
    ChunkNumber,
    UploadSessionUuid,
    ETag,
}
