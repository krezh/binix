//! Human-readable size parsing utilities.

use anyhow::{anyhow, Result};

/// Parses a human-readable size string like "64MB", "1GB", "512KiB" into bytes.
///
/// Supports both decimal (KB, MB, GB) and binary (KiB, MiB, GiB) units.
/// Can also parse plain numbers as bytes.
pub fn parse_size(s: &str) -> Result<usize> {
    let s = s.trim();

    // Try to parse as plain number (bytes)
    if let Ok(n) = s.parse::<usize>() {
        return Ok(n);
    }

    // Find where the numeric part ends
    let split_pos = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .ok_or_else(|| anyhow!("Invalid size format: {}", s))?;

    let (num_str, unit) = s.split_at(split_pos);
    let num: f64 = num_str
        .parse()
        .map_err(|_| anyhow!("Invalid number: {}", num_str))?;

    let unit = unit.trim().to_uppercase();
    let multiplier: u64 = match unit.as_str() {
        // Binary units (1024-based)
        "B" => 1,
        "KIB" | "K" => 1024,
        "MIB" | "M" => 1024 * 1024,
        "GIB" | "G" => 1024 * 1024 * 1024,
        "TIB" | "T" => 1024u64 * 1024 * 1024 * 1024,

        // Decimal units (1000-based)
        "KB" => 1000,
        "MB" => 1000 * 1000,
        "GB" => 1000 * 1000 * 1000,
        "TB" => 1000u64 * 1000 * 1000 * 1000,

        _ => {
            return Err(anyhow!(
                "Unknown unit: {}. Supported: B, KB, MB, GB, TB, KiB, MiB, GiB, TiB",
                unit
            ))
        }
    };

    let bytes = (num * multiplier as f64) as usize;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("1KB").unwrap(), 1000);
        assert_eq!(parse_size("1KiB").unwrap(), 1024);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("64MB").unwrap(), 64_000_000);
        assert_eq!(parse_size("64MiB").unwrap(), 67_108_864);
        assert_eq!(parse_size("64M").unwrap(), 67_108_864);
        assert_eq!(parse_size("1.5GB").unwrap(), 1_500_000_000);
        assert_eq!(parse_size("1.5GiB").unwrap(), 1_610_612_736);
        assert_eq!(parse_size("100 MB").unwrap(), 100_000_000);
    }

    #[test]
    fn test_parse_size_invalid() {
        assert!(parse_size("abc").is_err());
        assert!(parse_size("10XB").is_err());
        assert!(parse_size("").is_err());
    }
}
