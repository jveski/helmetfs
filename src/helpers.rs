//! Helper utilities: BLAKE3 checksums, `.sum` sidecar I/O, path predicates,
//! and errno mapping.

use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;

// ---------------------------------------------------------------------------
// BLAKE3 checksum
// ---------------------------------------------------------------------------

/// Compute BLAKE3 hash of a file, returned as 64-char lowercase hex string.
pub fn compute_blake3(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

// ---------------------------------------------------------------------------
// .sum sidecar files
// ---------------------------------------------------------------------------

/// Read a `.sum` sidecar file. Returns the 64-char hex digest (no newline).
pub fn read_sum_file(path: &Path) -> io::Result<String> {
    let content = fs::read_to_string(path)?;
    Ok(content.trim().to_string())
}

/// Write a `.sum` sidecar file. Writes hex + '\n' atomically-ish via rename.
pub fn write_sum_file(path: &Path, hex: &str) -> io::Result<()> {
    let tmp = path.with_extension("sum.tmp");
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(hex.as_bytes())?;
        f.write_all(b"\n")?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Return the `.sum` sidecar path for a given file path.
pub fn sum_path_for(path: &Path) -> std::path::PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".sum");
    std::path::PathBuf::from(s)
}

// ---------------------------------------------------------------------------
// Path predicates
// ---------------------------------------------------------------------------

/// A path is "hidden" if it should be invisible through the FUSE mount:
///  - The `.helmetfs` metadata directory
///  - Any `.sum` sidecar whose base file exists on disk
///
/// `rel_path` is relative to the backing directory (no leading slash).
/// `backing_dir` is the absolute path to the source directory.
pub fn is_hidden_path(rel_path: &str, backing_dir: &Path) -> bool {
    // .helmetfs directory itself or anything inside it
    if rel_path == ".helmetfs" || rel_path.starts_with(".helmetfs/") {
        return true;
    }

    // If the path ends with ".sum", hide it only when the corresponding
    // base file exists.  E.g. hide "foo.txt.sum" when "foo.txt" exists,
    // but leave "standalone.sum" visible if "standalone" doesn't exist.
    if let Some(base) = rel_path.strip_suffix(".sum") {
        if !base.is_empty() {
            let base_abs = backing_dir.join(base);
            if base_abs.exists() {
                return true;
            }
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Errno mapping
// ---------------------------------------------------------------------------

/// Map a `std::io::Error` to a positive errno value suitable for passing
/// to a FUSE `reply.error()` call.
pub fn io_error_to_errno(e: &io::Error) -> libc::c_int {
    e.raw_os_error().unwrap_or(libc::EIO)
}
