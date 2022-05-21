use chrono::NaiveDate;
use sqlx::types::Json;
use uuid::Uuid;

use crate::{errors::Error, errors::Result, DigestState, DistributionErrorCode};

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
    pub fn validate_range(&mut self, content_range: Option<(u64, u64)>) -> Result<()> {
        if let Some((begin, end)) = content_range {
            if begin != 0 && begin as i64 != self.last_range_end + 1 {
                return Err(Error::DistributionSpecError(
                    DistributionErrorCode::BlobUploadInvalid,
                ));
            }
            self.last_range_end = end as i64;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct Chunk {
    pub e_tag: Option<String>,
    pub chunk_number: i32,
}
