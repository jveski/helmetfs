//! FUSE operation callbacks and the `fuse_operations` constant.
//!
//! Each callback is an `unsafe extern "C"` function that translates between the
//! C FUSE interface and Rust.  State is accessed via `state::get_state()`.
//!
//! File handle encoding (same as Zig):
//!   bit 63       = write flag (1 = opened for writing)
//!   bits 0..62   = file descriptor

use std::ffi::{CStr, CString};
use std::os::unix::ffi::OsStrExt;

use libc::{
    c_char, c_int, c_uint, c_void, mode_t, off_t, size_t, EACCES, ENOENT,
    ENOTSUP, O_APPEND, O_CREAT, O_RDWR, O_WRONLY,
};

use crate::fuse_sys;
use crate::helpers;
use crate::state::{self, checksum_and_enqueue, FsState};

// ---------------------------------------------------------------------------
// File handle helpers
// ---------------------------------------------------------------------------

const WRITE_FLAG: u64 = 1 << 63;

fn encode_fh(fd: i32, writing: bool) -> u64 {
    let fh = fd as u64 & 0x7FFF_FFFF_FFFF_FFFF;
    if writing {
        fh | WRITE_FLAG
    } else {
        fh
    }
}

fn decode_fd(fh: u64) -> i32 {
    (fh & 0x7FFF_FFFF_FFFF_FFFF) as i32
}

fn is_write_fh(fh: u64) -> bool {
    fh & WRITE_FLAG != 0
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn get_state() -> &'static FsState {
    state::get_state()
}

/// Convert a C FUSE path to a relative path string.
unsafe fn c_path_to_rel(path: *const c_char) -> String {
    let cstr = CStr::from_ptr(path);
    let s = cstr.to_str().unwrap_or("");
    helpers::fuse_path_to_rel(s).to_string()
}

/// Convert a relative path to the absolute backing path as a CString.
fn backing_cpath(state: &FsState, rel: &str) -> CString {
    let abs = state.backing_path(rel);
    CString::new(abs.as_os_str().as_bytes()).unwrap_or_default()
}

fn neg_errno() -> c_int {
    -unsafe { *libc::__errno_location() }
}

// ---------------------------------------------------------------------------
// FUSE callbacks
// ---------------------------------------------------------------------------

unsafe extern "C" fn op_getattr(
    path: *const c_char,
    stbuf: *mut libc::stat,
    _fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();

    if helpers::is_hidden_path(&rel, &st.backing_dir) {
        return -ENOENT;
    }

    let abs = backing_cpath(st, &rel);
    if libc::lstat(abs.as_ptr(), stbuf) == -1 {
        return neg_errno();
    }
    0
}

unsafe extern "C" fn op_readlink(
    path: *const c_char,
    buf: *mut c_char,
    size: size_t,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();
    let abs = backing_cpath(st, &rel);
    let ret = libc::readlink(abs.as_ptr(), buf, size - 1);
    if ret == -1 {
        return neg_errno();
    }
    *buf.add(ret as usize) = 0; // null terminate
    0
}

unsafe extern "C" fn op_mknod(
    path: *const c_char,
    mode: mode_t,
    rdev: libc::dev_t,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();
    if helpers::is_hidden_path(&rel, &st.backing_dir) {
        return -EACCES;
    }
    let abs = backing_cpath(st, &rel);
    if libc::mknod(abs.as_ptr(), mode, rdev) == -1 {
        return neg_errno();
    }
    0
}

unsafe extern "C" fn op_mkdir(path: *const c_char, mode: mode_t) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();
    if helpers::is_hidden_path(&rel, &st.backing_dir) {
        return -EACCES;
    }
    let abs = backing_cpath(st, &rel);
    if libc::mkdir(abs.as_ptr(), mode) == -1 {
        return neg_errno();
    }
    0
}

unsafe extern "C" fn op_unlink(path: *const c_char) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();

    if helpers::is_hidden_path(&rel, &st.backing_dir) {
        return -ENOENT;
    }

    let abs = backing_cpath(st, &rel);
    if libc::unlink(abs.as_ptr()) == -1 {
        return neg_errno();
    }

    // Remove .sum sidecar
    let sum = helpers::sum_path_for(&st.backing_path(&rel));
    let _ = std::fs::remove_file(&sum);

    // Clean up path state
    st.remove_path_state(&rel);

    // Enqueue delete to replica
    st.repl_log.enqueue_delete(&rel);

    0
}

unsafe extern "C" fn op_rmdir(path: *const c_char) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();
    let abs = backing_cpath(st, &rel);
    if libc::rmdir(abs.as_ptr()) == -1 {
        return neg_errno();
    }
    0
}

unsafe extern "C" fn op_symlink(
    target: *const c_char,
    linkpath: *const c_char,
) -> c_int {
    let rel = c_path_to_rel(linkpath);
    let st = get_state();

    if helpers::is_hidden_path(&rel, &st.backing_dir) {
        return -EACCES;
    }

    let abs = backing_cpath(st, &rel);
    if libc::symlink(target, abs.as_ptr()) == -1 {
        return neg_errno();
    }

    // Enqueue symlink for replication (as a put — the replication worker
    // handles symlinks specially).
    st.repl_log.enqueue_put(&rel);

    0
}

unsafe extern "C" fn op_rename(
    from: *const c_char,
    to: *const c_char,
    _flags: c_uint,
) -> c_int {
    let rel_from = c_path_to_rel(from);
    let rel_to = c_path_to_rel(to);
    let st = get_state();

    let abs_from = backing_cpath(st, &rel_from);
    let abs_to = backing_cpath(st, &rel_to);

    if libc::rename(abs_from.as_ptr(), abs_to.as_ptr()) == -1 {
        return neg_errno();
    }

    // Move .sum sidecar if it exists
    let sum_from = helpers::sum_path_for(&st.backing_path(&rel_from));
    let sum_to = helpers::sum_path_for(&st.backing_path(&rel_to));
    if sum_from.exists() {
        let _ = std::fs::rename(&sum_from, &sum_to);
    }

    // Transfer path state
    {
        let mut map = st.path_state.write().unwrap();
        if let Some(info) = map.remove(&rel_from) {
            map.insert(rel_to.clone(), info);
        }
    }

    // Enqueue delete for old path, put for new path
    st.repl_log.enqueue_delete(&rel_from);

    // If the renamed file has a .sum, enqueue put directly.
    // Otherwise, checksum and enqueue.
    if sum_to.exists() {
        st.repl_log.enqueue_put(&rel_to);
    } else {
        // Regular file without .sum — compute checksum first
        let backing_to = st.backing_path(&rel_to);
        if backing_to.is_file() {
            checksum_and_enqueue(st, &rel_to);
        }
    }

    0
}

unsafe extern "C" fn op_link(
    _from: *const c_char,
    _to: *const c_char,
) -> c_int {
    -ENOTSUP
}

unsafe extern "C" fn op_chmod(
    path: *const c_char,
    mode: mode_t,
    fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();

    if !fi.is_null() {
        let fd = decode_fd((*fi).fh);
        if libc::fchmod(fd, mode) == -1 {
            return neg_errno();
        }
    } else {
        let abs = backing_cpath(st, &rel);
        if libc::chmod(abs.as_ptr(), mode) == -1 {
            return neg_errno();
        }
    }

    // chmod triggers re-replication (permissions changed)
    let backing = st.backing_path(&rel);
    if backing.is_file() {
        st.mark_dirty(&rel);
        state::checksum_if_idle(st, &rel);
    }

    0
}

unsafe extern "C" fn op_chown(
    path: *const c_char,
    uid: libc::uid_t,
    gid: libc::gid_t,
    fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();

    if !fi.is_null() {
        let fd = decode_fd((*fi).fh);
        if libc::fchown(fd, uid, gid) == -1 {
            return neg_errno();
        }
    } else {
        let abs = backing_cpath(st, &rel);
        if libc::lchown(abs.as_ptr(), uid, gid) == -1 {
            return neg_errno();
        }
    }
    0
}

unsafe extern "C" fn op_truncate(
    path: *const c_char,
    size: off_t,
    fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();

    if !fi.is_null() {
        let fd = decode_fd((*fi).fh);
        if libc::ftruncate(fd, size) == -1 {
            return neg_errno();
        }
    } else {
        let abs = backing_cpath(st, &rel);
        if libc::truncate(abs.as_ptr(), size) == -1 {
            return neg_errno();
        }
    }

    st.mark_dirty(&rel);
    state::checksum_if_idle(st, &rel);

    0
}

unsafe extern "C" fn op_open(
    path: *const c_char,
    fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();

    if helpers::is_hidden_path(&rel, &st.backing_dir) {
        return -ENOENT;
    }

    let abs = backing_cpath(st, &rel);
    let flags = (*fi).flags;
    let fd = libc::open(abs.as_ptr(), flags);
    if fd == -1 {
        return neg_errno();
    }

    let acc_mode = flags & libc::O_ACCMODE;
    let writing = acc_mode == O_WRONLY || acc_mode == O_RDWR || (flags & O_APPEND) != 0;

    if writing {
        st.inc_write_ref(&rel);
    }

    (*fi).fh = encode_fh(fd, writing);
    0
}

unsafe extern "C" fn op_read(
    _path: *const c_char,
    buf: *mut c_char,
    size: size_t,
    offset: off_t,
    fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let fd = decode_fd((*fi).fh);
    let ret = libc::pread(fd, buf as *mut c_void, size, offset);
    if ret == -1 {
        return neg_errno();
    }
    ret as c_int
}

unsafe extern "C" fn op_write(
    path: *const c_char,
    buf: *const c_char,
    size: size_t,
    offset: off_t,
    fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let fd = decode_fd((*fi).fh);
    let ret = libc::pwrite(fd, buf as *const c_void, size, offset);
    if ret == -1 {
        return neg_errno();
    }

    let rel = c_path_to_rel(path);
    get_state().mark_dirty(&rel);

    ret as c_int
}

unsafe extern "C" fn op_statfs(
    path: *const c_char,
    stbuf: *mut libc::statvfs,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();
    let abs = backing_cpath(st, &rel);
    if libc::statvfs(abs.as_ptr(), stbuf) == -1 {
        return neg_errno();
    }
    0
}

unsafe extern "C" fn op_flush(
    _path: *const c_char,
    fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let fd = decode_fd((*fi).fh);
    // flush = close(dup(fd)) to trigger any pending errors
    let dup_fd = libc::dup(fd);
    if dup_fd == -1 {
        return neg_errno();
    }
    if libc::close(dup_fd) == -1 {
        return neg_errno();
    }
    0
}

unsafe extern "C" fn op_release(
    path: *const c_char,
    fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let fh = (*fi).fh;
    let fd = decode_fd(fh);
    libc::close(fd);

    if is_write_fh(fh) {
        let rel = c_path_to_rel(path);
        let st = get_state();
        st.dec_write_ref(&rel);

        // If this was the last write handle and the file is dirty,
        // checksum and enqueue for replication.
        let info = st.get_path_info(&rel);
        if let Some(info) = info {
            if info.write_ref == 0 && info.dirty {
                checksum_and_enqueue(st, &rel);
            }
        }
    }

    0
}

unsafe extern "C" fn op_fsync(
    _path: *const c_char,
    datasync: c_int,
    fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let fd = decode_fd((*fi).fh);
    let ret = if datasync != 0 {
        libc::fdatasync(fd)
    } else {
        libc::fsync(fd)
    };
    if ret == -1 {
        return neg_errno();
    }
    0
}

unsafe extern "C" fn op_opendir(
    path: *const c_char,
    fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();

    if helpers::is_hidden_path(&rel, &st.backing_dir) {
        return -ENOENT;
    }

    let abs = backing_cpath(st, &rel);
    let dp = libc::opendir(abs.as_ptr());
    if dp.is_null() {
        return neg_errno();
    }
    (*fi).fh = dp as u64;
    0
}

unsafe extern "C" fn op_readdir(
    path: *const c_char,
    buf: *mut c_void,
    filler: fuse_sys::fuse_fill_dir_t,
    _offset: off_t,
    _fi: *mut fuse_sys::fuse_file_info,
    _flags: c_int,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();
    let abs_dir = st.backing_path(&rel);

    let filler = match filler {
        Some(f) => f,
        None => return -libc::EIO,
    };

    let entries = match std::fs::read_dir(&abs_dir) {
        Ok(e) => e,
        Err(e) => return helpers::io_error_to_errno(&e),
    };

    // Add . and ..
    let dot = CString::new(".").unwrap();
    let dotdot = CString::new("..").unwrap();
    filler(buf, dot.as_ptr(), std::ptr::null(), 0, 0);
    filler(buf, dotdot.as_ptr(), std::ptr::null(), 0, 0);

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Build relative path for this entry
        let entry_rel = if rel.is_empty() {
            name_str.to_string()
        } else {
            format!("{}/{}", rel, name_str)
        };

        // Skip hidden entries
        if helpers::is_hidden_path(&entry_rel, &st.backing_dir) {
            continue;
        }

        if let Ok(cname) = CString::new(name.as_bytes()) {
            filler(buf, cname.as_ptr(), std::ptr::null(), 0, 0);
        }
    }

    0
}

unsafe extern "C" fn op_releasedir(
    _path: *const c_char,
    fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let dp = (*fi).fh as *mut libc::DIR;
    if !dp.is_null() {
        libc::closedir(dp);
    }
    0
}

unsafe extern "C" fn op_access(
    path: *const c_char,
    mask: c_int,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();

    if helpers::is_hidden_path(&rel, &st.backing_dir) {
        return -ENOENT;
    }

    let abs = backing_cpath(st, &rel);
    if libc::access(abs.as_ptr(), mask) == -1 {
        return neg_errno();
    }
    0
}

unsafe extern "C" fn op_create(
    path: *const c_char,
    mode: mode_t,
    fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();

    if helpers::is_hidden_path(&rel, &st.backing_dir) {
        return -EACCES;
    }

    let abs = backing_cpath(st, &rel);
    let fd = libc::open(abs.as_ptr(), (*fi).flags | O_CREAT, mode as c_uint);
    if fd == -1 {
        return neg_errno();
    }

    st.inc_write_ref(&rel);
    st.mark_dirty(&rel);

    (*fi).fh = encode_fh(fd, true);
    0
}

unsafe extern "C" fn op_utimens(
    path: *const c_char,
    tv: *const libc::timespec,
    fi: *mut fuse_sys::fuse_file_info,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();

    if !fi.is_null() {
        let fd = decode_fd((*fi).fh);
        if libc::futimens(fd, tv) == -1 {
            return neg_errno();
        }
    } else {
        let abs = backing_cpath(st, &rel);
        if libc::utimensat(libc::AT_FDCWD, abs.as_ptr(), tv, libc::AT_SYMLINK_NOFOLLOW) == -1 {
            return neg_errno();
        }
    }
    0
}

unsafe extern "C" fn op_setxattr(
    path: *const c_char,
    name: *const c_char,
    value: *const c_char,
    size: size_t,
    flags: c_int,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();
    let abs = backing_cpath(st, &rel);
    if libc::setxattr(abs.as_ptr(), name, value as *const c_void, size, flags) == -1 {
        return neg_errno();
    }
    0
}

unsafe extern "C" fn op_getxattr(
    path: *const c_char,
    name: *const c_char,
    value: *mut c_char,
    size: size_t,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();
    let abs = backing_cpath(st, &rel);
    let ret = libc::getxattr(abs.as_ptr(), name, value as *mut c_void, size);
    if ret == -1 {
        return neg_errno();
    }
    ret as c_int
}

unsafe extern "C" fn op_listxattr(
    path: *const c_char,
    list: *mut c_char,
    size: size_t,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();
    let abs = backing_cpath(st, &rel);
    let ret = libc::listxattr(abs.as_ptr(), list, size);
    if ret == -1 {
        return neg_errno();
    }
    ret as c_int
}

unsafe extern "C" fn op_removexattr(
    path: *const c_char,
    name: *const c_char,
) -> c_int {
    let rel = c_path_to_rel(path);
    let st = get_state();
    let abs = backing_cpath(st, &rel);
    if libc::removexattr(abs.as_ptr(), name) == -1 {
        return neg_errno();
    }
    0
}

unsafe extern "C" fn op_init(
    _conn: *mut fuse_sys::fuse_conn_info,
    _cfg: *mut fuse_sys::fuse_config,
) -> *mut c_void {
    std::ptr::null_mut()
}

// ---------------------------------------------------------------------------
// fuse_operations constant
// ---------------------------------------------------------------------------

/// The static `fuse_operations` struct passed to `fuse_new`.
pub static FUSE_OPS: fuse_sys::fuse_operations = fuse_sys::fuse_operations {
    getattr: Some(op_getattr),
    readlink: Some(op_readlink),
    mknod: Some(op_mknod),
    mkdir: Some(op_mkdir),
    unlink: Some(op_unlink),
    rmdir: Some(op_rmdir),
    symlink: Some(op_symlink),
    rename: Some(op_rename),
    link: Some(op_link),
    chmod: Some(op_chmod),
    chown: Some(op_chown),
    truncate: Some(op_truncate),
    open: Some(op_open),
    read: Some(op_read),
    write: Some(op_write),
    statfs: Some(op_statfs),
    flush: Some(op_flush),
    release: Some(op_release),
    fsync: Some(op_fsync),
    setxattr: Some(op_setxattr),
    getxattr: Some(op_getxattr),
    listxattr: Some(op_listxattr),
    removexattr: Some(op_removexattr),
    opendir: Some(op_opendir),
    readdir: Some(op_readdir),
    releasedir: Some(op_releasedir),
    fsyncdir: None,
    init: Some(op_init),
    destroy: None,
    access: Some(op_access),
    create: Some(op_create),
    lock: None,
    utimens: Some(op_utimens),
    bmap: None,
    ioctl: None,
    poll: None,
    write_buf: None,
    read_buf: None,
    flock: None,
    fallocate: None,
    copy_file_range: None,
    lseek: None,
};
