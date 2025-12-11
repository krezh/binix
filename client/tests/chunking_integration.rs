//! Integration tests for client-side chunking functionality

use binix::chunking::chunk_stream;
use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use std::io::Cursor;

/// Test that chunking produces the expected number of chunks
#[tokio::test]
async fn test_chunking_produces_chunks() {
    // Create test data (1MB of data)
    let test_data = vec![0x42u8; 1024 * 1024];
    let cursor = Cursor::new(&test_data);

    // Chunk with 64KB min, 128KB avg, 256KB max
    let min_size = 64 * 1024;
    let avg_size = 128 * 1024;
    let max_size = 256 * 1024;

    let mut chunks = chunk_stream(cursor, min_size, avg_size, max_size);

    let mut chunk_count = 0;
    let mut total_size = 0;

    while let Some(chunk) = chunks.try_next().await.unwrap() {
        chunk_count += 1;
        total_size += chunk.len();

        // Verify chunk is within bounds
        assert!(chunk.len() <= max_size, "Chunk exceeds max size");
    }

    // Verify we got multiple chunks
    assert!(chunk_count > 1, "Should produce multiple chunks");

    // Verify total size matches input
    assert_eq!(
        total_size,
        test_data.len(),
        "Total chunk size should match input"
    );
}

/// Test that chunks can be reassembled to original data
#[tokio::test]
async fn test_chunking_preserves_data() {
    // Create test data with a pattern
    let mut test_data = Vec::new();
    for i in 0..1024 * 512 {
        test_data.push((i % 256) as u8);
    }

    let cursor = Cursor::new(&test_data);

    let min_size = 32 * 1024;
    let avg_size = 64 * 1024;
    let max_size = 128 * 1024;

    let mut chunks = chunk_stream(cursor, min_size, avg_size, max_size);

    // Collect all chunks
    let mut reassembled = Vec::new();
    while let Some(chunk) = chunks.try_next().await.unwrap() {
        reassembled.extend_from_slice(&chunk);
    }

    // Verify data is identical
    assert_eq!(
        reassembled, test_data,
        "Reassembled data should match original"
    );
}

/// Test chunking with different size configurations
#[tokio::test]
async fn test_chunking_different_sizes() {
    let test_data = vec![0xAAu8; 2 * 1024 * 1024]; // 2MB

    let test_cases = vec![
        (16 * 1024, 32 * 1024, 64 * 1024),     // Small chunks
        (64 * 1024, 128 * 1024, 256 * 1024),   // Medium chunks
        (256 * 1024, 512 * 1024, 1024 * 1024), // Large chunks
    ];

    for (min, avg, max) in test_cases {
        let cursor = Cursor::new(&test_data);
        let mut chunks = chunk_stream(cursor, min, avg, max);

        let mut chunk_sizes = Vec::new();
        while let Some(chunk) = chunks.try_next().await.unwrap() {
            chunk_sizes.push(chunk.len());
        }

        // Verify all chunks are within bounds
        for size in &chunk_sizes {
            assert!(*size <= max, "Chunk size {} exceeds max {}", size, max);
        }

        // Verify total size
        let total: usize = chunk_sizes.iter().sum();
        assert_eq!(total, test_data.len(), "Total size mismatch");
    }
}

/// Test that chunks have consistent hashes
#[tokio::test]
async fn test_chunk_hashing() {
    let test_data = vec![0x55u8; 512 * 1024]; // 512KB

    let cursor = Cursor::new(&test_data);
    let min_size = 64 * 1024;
    let avg_size = 128 * 1024;
    let max_size = 256 * 1024;

    let mut chunks = chunk_stream(cursor, min_size, avg_size, max_size);

    let mut chunk_hashes = Vec::new();
    while let Some(chunk) = chunks.try_next().await.unwrap() {
        let mut hasher = Sha256::new();
        hasher.update(&chunk);
        let hash = hasher.finalize();
        chunk_hashes.push(hash.to_vec());
    }

    // Verify we got multiple chunks
    assert!(chunk_hashes.len() > 1, "Should produce multiple chunks");

    // Re-chunk the same data and verify hashes are identical
    let cursor = Cursor::new(&test_data);
    let mut chunks = chunk_stream(cursor, min_size, avg_size, max_size);

    let mut idx = 0;
    while let Some(chunk) = chunks.try_next().await.unwrap() {
        let mut hasher = Sha256::new();
        hasher.update(&chunk);
        let hash = hasher.finalize();

        assert_eq!(
            hash.as_slice(),
            chunk_hashes[idx].as_slice(),
            "Chunk {} hash should be consistent",
            idx
        );
        idx += 1;
    }

    assert_eq!(
        idx,
        chunk_hashes.len(),
        "Should produce same number of chunks"
    );
}

/// Test chunking with small files (below threshold)
#[tokio::test]
async fn test_small_file_chunking() {
    // Small file (10KB)
    let test_data = vec![0x77u8; 10 * 1024];

    let cursor = Cursor::new(&test_data);
    let min_size = 64 * 1024;
    let avg_size = 128 * 1024;
    let max_size = 256 * 1024;

    let mut chunks = chunk_stream(cursor, min_size, avg_size, max_size);

    // Should produce exactly 1 chunk (file is too small to split)
    let chunk = chunks
        .try_next()
        .await
        .unwrap()
        .expect("Should have one chunk");
    assert_eq!(
        chunk.len(),
        test_data.len(),
        "Single chunk should be whole file"
    );

    // Should be no more chunks
    assert!(
        chunks.try_next().await.unwrap().is_none(),
        "Should only have one chunk"
    );
}
