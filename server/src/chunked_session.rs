use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use tokio::io::AsyncBufRead;
use tokio::sync::Mutex;
use uuid::Uuid;

use binix::api::v1::upload_path::UploadPathNarInfo;

use crate::config::ChunkedUploadConfig;

/// A chunked upload session.
#[derive(Debug)]
pub struct ChunkedUploadSession {
    /// Unique session ID.
    pub session_id: String,
    /// Cache ID this upload belongs to.
    pub cache_id: i64,
    /// NAR info for this upload.
    pub nar_info: UploadPathNarInfo,
    /// Total number of chunks expected.
    pub total_chunks: u32,
    /// Size of each chunk (except possibly the last).
    pub chunk_size: usize,
    /// Chunks stored in memory.
    chunks: HashMap<u32, Bytes>,
    /// Set tracking which chunks have been received.
    received_chunks: HashSet<u32>,
    /// When this session was created.
    pub created_at: DateTime<Utc>,
}

impl ChunkedUploadSession {
    /// Creates a new chunked upload session.
    fn new(
        session_id: String,
        cache_id: i64,
        nar_info: UploadPathNarInfo,
        total_chunks: u32,
        chunk_size: usize,
    ) -> Self {
        Self {
            session_id,
            cache_id,
            nar_info,
            total_chunks,
            chunk_size,
            chunks: HashMap::new(),
            received_chunks: HashSet::new(),
            created_at: Utc::now(),
        }
    }

    /// Writes a chunk to storage.
    fn write_chunk(&mut self, chunk_index: u32, chunk_data: Bytes) -> Result<()> {
        if chunk_index >= self.total_chunks {
            return Err(anyhow!(
                "Chunk index {} exceeds total chunks {}",
                chunk_index,
                self.total_chunks
            ));
        }

        self.chunks.insert(chunk_index, chunk_data);
        self.received_chunks.insert(chunk_index);
        Ok(())
    }

    /// Checks if all chunks have been received.
    fn is_complete(&self) -> bool {
        self.received_chunks.len() == self.total_chunks as usize
    }

    /// Gets the reassembled NAR stream.
    fn get_reassembled_stream(self) -> Result<Box<dyn AsyncBufRead + Send + Unpin>> {
        let mut buffer = Vec::with_capacity(self.nar_info.nar_size);

        for i in 0..self.total_chunks {
            let chunk = self
                .chunks
                .get(&i)
                .ok_or_else(|| anyhow!("Missing chunk {}", i))?;
            buffer.extend_from_slice(chunk);
        }

        Ok(Box::new(Cursor::new(buffer)))
    }
}

/// Manages chunked upload sessions.
#[derive(Debug)]
pub struct ChunkedSessionManager {
    sessions: Arc<Mutex<HashMap<String, ChunkedUploadSession>>>,
    config: ChunkedUploadConfig,
}

impl ChunkedSessionManager {
    /// Creates a new session manager.
    pub fn new(config: ChunkedUploadConfig) -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Checks if a session exists.
    pub async fn session_exists(&self, session_id: &str) -> bool {
        let sessions = self.sessions.lock().await;
        sessions.contains_key(session_id)
    }

    /// Creates a new upload session.
    pub async fn create_session(
        &self,
        session_id: String,
        cache_id: i64,
        nar_info: UploadPathNarInfo,
        total_chunks: u32,
        chunk_size: usize,
    ) -> Result<String> {
        let session = ChunkedUploadSession::new(
            session_id.clone(),
            cache_id,
            nar_info,
            total_chunks,
            chunk_size,
        );

        let mut sessions = self.sessions.lock().await;
        sessions.insert(session_id.clone(), session);

        Ok(session_id)
    }

    /// Writes a chunk to a session.
    pub async fn write_chunk(
        &self,
        session_id: &str,
        chunk_index: u32,
        chunk_data: Bytes,
    ) -> Result<()> {
        let mut sessions = self.sessions.lock().await;
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| anyhow!("Session not found: {}", session_id))?;

        session.write_chunk(chunk_index, chunk_data)
    }

    /// Checks if a session is complete.
    pub async fn is_complete(&self, session_id: &str) -> Result<bool> {
        let sessions = self.sessions.lock().await;
        let session = sessions
            .get(session_id)
            .ok_or_else(|| anyhow!("Session not found: {}", session_id))?;

        Ok(session.is_complete())
    }

    /// Gets the reassembled NAR stream and removes the session.
    pub async fn get_reassembled_stream(
        &self,
        session_id: &str,
    ) -> Result<Box<dyn AsyncBufRead + Send + Unpin>> {
        let mut sessions = self.sessions.lock().await;
        let session = sessions
            .remove(session_id)
            .ok_or_else(|| anyhow!("Session not found: {}", session_id))?;

        session.get_reassembled_stream()
    }

    /// Cleans up a session.
    pub async fn cleanup_session(&self, session_id: &str) -> Result<()> {
        let mut sessions = self.sessions.lock().await;
        sessions.remove(session_id);
        Ok(())
    }

    /// Cleans up expired sessions.
    pub async fn cleanup_expired(&self) -> Result<usize> {
        let timeout = Duration::seconds(self.config.session_timeout_secs as i64);
        let cutoff = Utc::now() - timeout;

        let mut sessions = self.sessions.lock().await;
        let expired_ids: Vec<String> = sessions
            .iter()
            .filter(|(_, session)| session.created_at < cutoff)
            .map(|(id, _)| id.clone())
            .collect();

        let count = expired_ids.len();

        for session_id in expired_ids {
            sessions.remove(&session_id);
        }

        Ok(count)
    }

    /// Gets the current number of active sessions.
    pub async fn active_sessions(&self) -> usize {
        let sessions = self.sessions.lock().await;
        sessions.len()
    }
}
