use std::io::Cursor;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use chrono::{Duration, Utc};
use sea_orm::entity::prelude::*;
use sea_orm::{ActiveValue::Set, DatabaseConnection};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufRead;

use binix::api::v1::upload_path::UploadPathNarInfo;

use crate::config::ChunkedUploadConfig;
use crate::database::entity::chunked_upload_session::{self, Entity as ChunkedUploadSessionEntity};

/// Information about a CDC chunk uploaded to S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredChunkInfo {
    #[serde(default)]
    pub source_chunk_index: u32,
    #[serde(default)]
    pub position_in_chunk: u32,
    pub chunk_id: i64,
    pub chunk_hash: String,
    pub compression: String,
}

/// Manages chunked upload sessions using PostgreSQL.
#[derive(Clone, Debug)]
pub struct ChunkedSessionManager {
    config: ChunkedUploadConfig,
}

impl ChunkedSessionManager {
    /// Creates a new session manager.
    pub fn new(config: ChunkedUploadConfig) -> Self {
        Self { config }
    }

    /// Checks if a session exists.
    pub async fn session_exists(&self, session_id: &str, db: &DatabaseConnection) -> Result<bool> {
        let session = ChunkedUploadSessionEntity::find_by_id(session_id)
            .one(db)
            .await?;
        Ok(session.is_some())
    }

    /// Validates that session parameters match expected values.
    pub async fn validate_session(
        &self,
        session_id: &str,
        total_chunks: u32,
        nar_size: usize,
        db: &DatabaseConnection,
    ) -> Result<()> {
        let session = ChunkedUploadSessionEntity::find_by_id(session_id)
            .one(db)
            .await?
            .ok_or_else(|| anyhow!("Session not found: {}", session_id))?;

        if session.total_chunks != total_chunks as i32 {
            return Err(anyhow!(
                "Session total_chunks mismatch: expected {}, got {}",
                session.total_chunks,
                total_chunks
            ));
        }

        let nar_info: UploadPathNarInfo = serde_json::from_str(&session.nar_info)?;
        if nar_info.nar_size != nar_size {
            return Err(anyhow!(
                "Session nar_size mismatch: expected {}, got {}",
                nar_info.nar_size,
                nar_size
            ));
        }

        Ok(())
    }

    /// Creates a new upload session.
    pub async fn create_session(
        &self,
        session_id: String,
        cache_id: i64,
        nar_info: UploadPathNarInfo,
        total_chunks: u32,
        db: &DatabaseConnection,
    ) -> Result<String> {
        let now = Utc::now().naive_utc();
        let nar_info_json = serde_json::to_string(&nar_info)?;

        tracing::info!(
            "Creating chunked upload session {} for NAR {} with {} chunks",
            session_id,
            nar_info.nar_hash.to_typed_base16(),
            total_chunks
        );

        let model = chunked_upload_session::ActiveModel {
            session_id: Set(session_id.clone()),
            cache_id: Set(cache_id),
            nar_info: Set(nar_info_json),
            total_chunks: Set(total_chunks as i32),
            received_chunks: Set(String::new()),
            cdc_chunks: Set(String::new()),
            total_bytes: Set(0),
            created_at: Set(now),
            last_activity: Set(now),
        };

        // Use ON CONFLICT DO NOTHING to make this idempotent
        match chunked_upload_session::Entity::insert(model)
            .exec(db)
            .await
        {
            Ok(_) => Ok(session_id),
            Err(e) => {
                // Check if this is a duplicate key error (session already exists)
                // If so, that's fine - another chunk created it first
                if e.to_string().contains("duplicate key") || e.to_string().contains("UNIQUE constraint") {
                    Ok(session_id)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// Prepares to receive a chunk (validates and creates stream).
    pub async fn prepare_chunk_stream(
        &self,
        session_id: &str,
        chunk_index: u32,
        chunk_data: &Bytes,
        db: &DatabaseConnection,
    ) -> Result<(Box<dyn AsyncBufRead + Send + Unpin>, bool)> {
        let session = ChunkedUploadSessionEntity::find_by_id(session_id)
            .one(db)
            .await?
            .ok_or_else(|| anyhow!("Session not found: {}", session_id))?;

        let received: Vec<u32> = if session.received_chunks.is_empty() {
            Vec::new()
        } else {
            session
                .received_chunks
                .split(',')
                .filter_map(|s| s.parse().ok())
                .collect()
        };

        // Check if already received (idempotency)
        if received.contains(&chunk_index) {
            return Ok((Box::new(Cursor::new(Bytes::new())), true));
        }

        // Validate chunk index is within bounds
        if chunk_index >= session.total_chunks as u32 {
            return Err(anyhow!(
                "Chunk index {} exceeds total chunks {}",
                chunk_index,
                session.total_chunks
            ));
        }

        // Update last activity
        let mut active_model: chunked_upload_session::ActiveModel = session.into();
        active_model.last_activity = Set(Utc::now().naive_utc());
        active_model.update(db).await?;

        Ok((Box::new(Cursor::new(chunk_data.clone())), false))
    }

    /// Marks a chunk as successfully received and stores CDC chunks.
    pub async fn complete_chunk(
        &self,
        session_id: &str,
        chunk_index: u32,
        chunk_data: &Bytes,
        cdc_chunks: Vec<StoredChunkInfo>,
        db: &DatabaseConnection,
    ) -> Result<()> {
        use sea_orm::{ConnectionTrait, Statement, TransactionTrait};

        let txn = db.begin().await?;

        // Lock the row for update to prevent concurrent modifications
        let session = txn
            .query_one(Statement::from_sql_and_values(
                db.get_database_backend(),
                "SELECT * FROM chunked_upload_session WHERE session_id = $1 FOR UPDATE",
                [session_id.into()],
            ))
            .await?
            .ok_or_else(|| anyhow!("Session not found: {}", session_id))?;

        let current_cdc_chunks: String = session.try_get("", "cdc_chunks")?;
        let current_received: String = session.try_get("", "received_chunks")?;
        let current_total_bytes: i64 = session.try_get("", "total_bytes")?;

        // Parse existing CDC chunks
        let mut all_cdc_chunks: Vec<StoredChunkInfo> = if current_cdc_chunks.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&current_cdc_chunks)?
        };

        // Append new CDC chunks
        all_cdc_chunks.extend(cdc_chunks);

        // Parse existing received chunks
        let mut received: Vec<u32> = if current_received.is_empty() {
            Vec::new()
        } else {
            current_received
                .split(',')
                .filter_map(|s| s.parse().ok())
                .collect()
        };

        // Update state
        received.push(chunk_index);
        let new_total_bytes = current_total_bytes + chunk_data.len() as i64;

        let received_str = received
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let cdc_chunks_json = serde_json::to_string(&all_cdc_chunks)?;

        // Update database within the transaction
        txn.execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            "UPDATE chunked_upload_session SET received_chunks = $1, cdc_chunks = $2, total_bytes = $3, last_activity = $4 WHERE session_id = $5",
            [
                received_str.into(),
                cdc_chunks_json.into(),
                new_total_bytes.into(),
                Utc::now().naive_utc().into(),
                session_id.into(),
            ],
        ))
        .await?;

        txn.commit().await?;

        Ok(())
    }

    /// Checks if upload is complete.
    pub async fn is_complete(&self, session_id: &str, db: &DatabaseConnection) -> Result<bool> {
        let session = ChunkedUploadSessionEntity::find_by_id(session_id)
            .one(db)
            .await?
            .ok_or_else(|| anyhow!("Session not found: {}", session_id))?;

        let received_count = if session.received_chunks.is_empty() {
            0
        } else {
            session.received_chunks.split(',').count()
        };

        Ok(received_count == session.total_chunks as usize)
    }

    /// Finalizes a session and returns CDC chunks for validation.
    ///
    /// Uses row locking and deletes the session atomically to prevent race conditions
    /// where multiple pods try to finalize the same session.
    ///
    /// Returns (cache_id, nar_info, cdc_chunks) for the caller to validate and create NAR.
    pub async fn finalize_session(
        &self,
        session_id: &str,
        db: &DatabaseConnection,
    ) -> Result<(i64, UploadPathNarInfo, Vec<StoredChunkInfo>)> {
        use sea_orm::{ConnectionTrait, Statement, TransactionTrait};

        let txn = db.begin().await?;

        // Lock the row to prevent concurrent finalization
        let session = txn
            .query_one(Statement::from_sql_and_values(
                db.get_database_backend(),
                "SELECT * FROM chunked_upload_session WHERE session_id = $1 FOR UPDATE",
                [session_id.into()],
            ))
            .await?
            .ok_or_else(|| anyhow!("Session not found or already finalized: {}", session_id))?;

        let cache_id: i64 = session.try_get("", "cache_id")?;
        let nar_info_json: String = session.try_get("", "nar_info")?;
        let total_chunks: i32 = session.try_get("", "total_chunks")?;
        let received_chunks: String = session.try_get("", "received_chunks")?;
        let cdc_chunks_json: String = session.try_get("", "cdc_chunks")?;
        let total_bytes: i64 = session.try_get("", "total_bytes")?;

        tracing::info!(
            "Finalizing chunked upload session {} for NAR",
            session_id
        );

        let nar_info: UploadPathNarInfo = serde_json::from_str(&nar_info_json)?;

        // Validate completeness
        let received_count = if received_chunks.is_empty() {
            0
        } else {
            received_chunks.split(',').count()
        };

        if received_count != total_chunks as usize {
            txn.rollback().await?;
            return Err(anyhow!(
                "Session incomplete: {}/{} chunks received",
                received_count,
                total_chunks
            ));
        }

        // Validate size
        if total_bytes != nar_info.nar_size as i64 {
            txn.rollback().await?;
            return Err(anyhow!(
                "NAR size mismatch: expected {}, got {}",
                nar_info.nar_size,
                total_bytes
            ));
        }

        // Parse CDC chunks
        let cdc_chunks: Vec<StoredChunkInfo> = if cdc_chunks_json.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&cdc_chunks_json)?
        };

        // Delete the session atomically to prevent other pods from finalizing
        txn.execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            "DELETE FROM chunked_upload_session WHERE session_id = $1",
            [session_id.into()],
        ))
        .await?;

        txn.commit().await?;

        tracing::info!(
            "All {} client chunks received for session {}, {} CDC chunks to validate",
            received_count,
            session_id,
            cdc_chunks.len()
        );

        Ok((cache_id, nar_info, cdc_chunks))
    }

    /// Removes expired sessions based on last activity.
    pub async fn cleanup_expired(&self, db: &DatabaseConnection) -> Result<usize> {
        let timeout = Duration::seconds(self.config.session_timeout_secs as i64);
        let cutoff = (Utc::now() - timeout).naive_utc();

        let result = ChunkedUploadSessionEntity::delete_many()
            .filter(chunked_upload_session::Column::LastActivity.lt(cutoff))
            .exec(db)
            .await?;

        Ok(result.rows_affected as usize)
    }
}
