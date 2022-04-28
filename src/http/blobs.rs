use chrono::NaiveDate;
use sqlx::types::Json;
use uuid::Uuid;

use crate::DigestState;

pub struct UploadSession {
    pub uuid: Uuid,
    pub start_date: NaiveDate,
    pub digest_state: Option<Json<DigestState>>,
}
