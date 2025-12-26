//! Chunked upload session entity.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "chunked_upload_session")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub session_id: String,
    pub cache_id: i64,
    /// Full nar_info JSON
    pub nar_info: String,
    pub total_chunks: i32,
    /// Comma-separated list of received chunk indices
    pub received_chunks: String,
    /// JSON array of CDC chunk info
    pub cdc_chunks: String,
    pub total_bytes: i64,
    pub created_at: DateTime,
    pub last_activity: DateTime,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
