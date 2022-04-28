use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub upload_id: String,
    pub part_number: i32,
    pub last_range_end: u64,
}

impl ChunkInfo {
    pub fn inc(&mut self) {
        self.part_number += 1;
    }
}
