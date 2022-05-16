use uuid::Uuid;
use chrono::NaiveDate;
use sqlx::types::Json;

use crate::DigestState;

pub struct UploadSession {
    pub uuid: Uuid,
    pub start_date: NaiveDate,
    pub upload_id: Option<String>,
    pub chunk_number: i32,
    pub last_range_end: i64,
    pub digest_state: Option<Json<DigestState>>,
}

#[derive(Default)]
pub struct Chunk {
    pub e_tag: Option<String>,
    pub chunk_number: i32,
}

