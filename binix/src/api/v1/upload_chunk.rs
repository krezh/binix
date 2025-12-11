use serde::{Deserialize, Serialize};

use crate::cache::CacheName;
use crate::hash::Hash;

/// Header containing the chunk hash.
pub const BINIX_CHUNK_HASH: &str = "X-Binix-Chunk-Hash";

/// Header containing the chunk size.
pub const BINIX_CHUNK_SIZE: &str = "X-Binix-Chunk-Size";

/// Response from uploading a chunk.
#[derive(Debug, Serialize, Deserialize)]
pub struct UploadChunkResponse {
    /// Whether the chunk was deduplicated.
    pub deduplicated: bool,

    /// The chunk ID assigned by the server.
    pub chunk_id: i64,

    /// The compressed size of the chunk.
    pub file_size: Option<usize>,
}

/// Request to finalize a chunked NAR upload.
#[derive(Debug, Serialize, Deserialize)]
pub struct FinalizeNarRequest {
    /// The name of the binary cache to upload to.
    pub cache: CacheName,

    /// The hash portion of the store path.
    pub store_path_hash: String,

    /// The full store path being cached, including the store directory.
    pub store_path: String,

    /// Other store paths this object directly references.
    pub references: Vec<String>,

    /// The system this derivation is built for.
    pub system: Option<String>,

    /// The derivation that produced this object.
    pub deriver: Option<String>,

    /// The signatures of this object.
    pub sigs: Vec<String>,

    /// The CA field of this object.
    pub ca: Option<String>,

    /// The hash of the NAR.
    ///
    /// It must begin with `sha256:` with the SHA-256 hash in the
    /// hexadecimal format (64 hex characters).
    pub nar_hash: Hash,

    /// The size of the NAR.
    pub nar_size: usize,

    /// The ordered list of chunk hashes that make up this NAR.
    pub chunk_hashes: Vec<Hash>,
}

/// Response from finalizing a chunked NAR upload.
#[derive(Debug, Serialize, Deserialize)]
pub struct FinalizeNarResponse {
    /// The total compressed size of all chunks.
    pub file_size: usize,

    /// The fraction of data that was deduplicated, from 0 to 1.
    pub frac_deduplicated: Option<f64>,
}
