//! Raw FFI declarations for libfuse3.
//!
//! We link directly against libfuse3 and call versioned symbols where needed.
//! All structs are laid out to match the C ABI exactly.

#![allow(non_camel_case_types, dead_code)]

use libc::{
    c_char, c_int, c_uint, c_void, dev_t, gid_t, mode_t, off_t, size_t, ssize_t, uid_t,
};

// ---------------------------------------------------------------------------
// Opaque types
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct fuse {
    _private: [u8; 0],
}

#[repr(C)]
pub struct fuse_session {
    _private: [u8; 0],
}

#[repr(C)]
pub struct fuse_pollhandle {
    _private: [u8; 0],
}

#[repr(C)]
pub struct fuse_conn_info {
    pub proto_major: c_uint,
    pub proto_minor: c_uint,
    pub max_write: c_uint,
    pub max_read: c_uint,
    pub max_readahead: c_uint,
    pub capable: c_uint,
    pub want: c_uint,
    pub max_background: c_uint,
    pub congestion_threshold: c_uint,
    pub time_gran: c_uint,
    pub reserved: [c_uint; 22],
}

#[repr(C)]
pub struct fuse_config {
    pub set_gid: c_int,
    pub gid: c_uint,
    pub set_uid: c_int,
    pub uid: c_uint,
    pub set_mode: c_int,
    pub umask: c_uint,
    pub entry_timeout: f64,
    pub negative_timeout: f64,
    pub attr_timeout: f64,
    pub intr: c_int,
    pub intr_signal: c_int,
    pub remember: c_int,
    pub hard_remove: c_int,
    pub use_ino: c_int,
    pub readdir_ino: c_int,
    pub direct_io: c_int,
    pub kernel_cache: c_int,
    pub auto_cache: c_int,
    pub no_rofd_flush: c_int,
    pub ac_attr_timeout_set: c_int,
    pub ac_attr_timeout: f64,
    pub nullpath_ok: c_int,
    pub show_help: c_int,
    pub modules: *mut c_char,
    pub debug: c_int,
}

// ---------------------------------------------------------------------------
// fuse_file_info — C bitfields packed into u32
// ---------------------------------------------------------------------------

/// Matches `struct fuse_file_info` from fuse_common.h.
///
/// The C struct uses bitfields for flags between `flags` and `fh`.  We replace
/// the two groups of bitfields with two `u32` fields to get the correct size
/// and alignment without needing Rust bitfield support.
#[repr(C)]
pub struct fuse_file_info {
    pub flags: c_int,
    /// Bitfields: writepage(1), direct_io(1), keep_cache(1), flush(1),
    /// nonseekable(1), flock_release(1), cache_readdir(1), noflush(1),
    /// padding(24)
    pub bitfields: u32,
    /// padding2: 32 bits
    pub padding2: u32,
    pub fh: u64,
    pub lock_owner: u64,
    pub poll_events: u32,
}

// ---------------------------------------------------------------------------
// fuse_args
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct fuse_args {
    pub argc: c_int,
    pub argv: *mut *mut c_char,
    pub allocated: c_int,
}

// ---------------------------------------------------------------------------
// fuse_bufvec (for write_buf / read_buf — we don't implement these, but need
// the type for the operations struct)
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct fuse_buf {
    pub size: size_t,
    pub flags: c_int,
    pub mem: *mut c_void,
    pub fd: c_int,
    pub pos: off_t,
}

#[repr(C)]
pub struct fuse_bufvec {
    pub count: size_t,
    pub idx: size_t,
    pub off: size_t,
    pub buf: [fuse_buf; 1],
}

// ---------------------------------------------------------------------------
// fuse_loop_config (opaque on fuse >= 3.12)
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct fuse_loop_config {
    _private: [u8; 0],
}

// ---------------------------------------------------------------------------
// fuse_fill_dir_t
// ---------------------------------------------------------------------------

pub type fuse_fill_dir_t = Option<
    unsafe extern "C" fn(
        buf: *mut c_void,
        name: *const c_char,
        stbuf: *const libc::stat,
        off: off_t,
        flags: c_int,
    ) -> c_int,
>;

// ---------------------------------------------------------------------------
// fuse_operations — all 43 function-pointer fields
// ---------------------------------------------------------------------------

/// Must match `struct fuse_operations` exactly (field order + count).
/// Unused callbacks are set to `None`.
#[repr(C)]
pub struct fuse_operations {
    pub getattr:
        Option<unsafe extern "C" fn(*const c_char, *mut libc::stat, *mut fuse_file_info) -> c_int>,
    pub readlink: Option<unsafe extern "C" fn(*const c_char, *mut c_char, size_t) -> c_int>,
    pub mknod: Option<unsafe extern "C" fn(*const c_char, mode_t, dev_t) -> c_int>,
    pub mkdir: Option<unsafe extern "C" fn(*const c_char, mode_t) -> c_int>,
    pub unlink: Option<unsafe extern "C" fn(*const c_char) -> c_int>,
    pub rmdir: Option<unsafe extern "C" fn(*const c_char) -> c_int>,
    pub symlink: Option<unsafe extern "C" fn(*const c_char, *const c_char) -> c_int>,
    pub rename: Option<unsafe extern "C" fn(*const c_char, *const c_char, c_uint) -> c_int>,
    pub link: Option<unsafe extern "C" fn(*const c_char, *const c_char) -> c_int>,
    pub chmod:
        Option<unsafe extern "C" fn(*const c_char, mode_t, *mut fuse_file_info) -> c_int>,
    pub chown: Option<
        unsafe extern "C" fn(*const c_char, uid_t, gid_t, *mut fuse_file_info) -> c_int,
    >,
    pub truncate:
        Option<unsafe extern "C" fn(*const c_char, off_t, *mut fuse_file_info) -> c_int>,
    pub open: Option<unsafe extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub read: Option<
        unsafe extern "C" fn(
            *const c_char,
            *mut c_char,
            size_t,
            off_t,
            *mut fuse_file_info,
        ) -> c_int,
    >,
    pub write: Option<
        unsafe extern "C" fn(
            *const c_char,
            *const c_char,
            size_t,
            off_t,
            *mut fuse_file_info,
        ) -> c_int,
    >,
    pub statfs: Option<unsafe extern "C" fn(*const c_char, *mut libc::statvfs) -> c_int>,
    pub flush: Option<unsafe extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub release: Option<unsafe extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub fsync:
        Option<unsafe extern "C" fn(*const c_char, c_int, *mut fuse_file_info) -> c_int>,
    pub setxattr: Option<
        unsafe extern "C" fn(*const c_char, *const c_char, *const c_char, size_t, c_int) -> c_int,
    >,
    pub getxattr:
        Option<unsafe extern "C" fn(*const c_char, *const c_char, *mut c_char, size_t) -> c_int>,
    pub listxattr: Option<unsafe extern "C" fn(*const c_char, *mut c_char, size_t) -> c_int>,
    pub removexattr: Option<unsafe extern "C" fn(*const c_char, *const c_char) -> c_int>,
    pub opendir: Option<unsafe extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub readdir: Option<
        unsafe extern "C" fn(
            *const c_char,
            *mut c_void,
            fuse_fill_dir_t,
            off_t,
            *mut fuse_file_info,
            c_int,
        ) -> c_int,
    >,
    pub releasedir: Option<unsafe extern "C" fn(*const c_char, *mut fuse_file_info) -> c_int>,
    pub fsyncdir:
        Option<unsafe extern "C" fn(*const c_char, c_int, *mut fuse_file_info) -> c_int>,
    pub init: Option<
        unsafe extern "C" fn(*mut fuse_conn_info, *mut fuse_config) -> *mut c_void,
    >,
    pub destroy: Option<unsafe extern "C" fn(*mut c_void)>,
    pub access: Option<unsafe extern "C" fn(*const c_char, c_int) -> c_int>,
    pub create:
        Option<unsafe extern "C" fn(*const c_char, mode_t, *mut fuse_file_info) -> c_int>,
    pub lock: Option<
        unsafe extern "C" fn(*const c_char, *mut fuse_file_info, c_int, *mut libc::flock) -> c_int,
    >,
    pub utimens: Option<
        unsafe extern "C" fn(
            *const c_char,
            *const libc::timespec,
            *mut fuse_file_info,
        ) -> c_int,
    >,
    pub bmap: Option<unsafe extern "C" fn(*const c_char, size_t, *mut u64) -> c_int>,
    // ioctl — use unsigned int cmd (FUSE_USE_VERSION >= 35)
    pub ioctl: Option<
        unsafe extern "C" fn(
            *const c_char,
            c_uint,
            *mut c_void,
            *mut fuse_file_info,
            c_uint,
            *mut c_void,
        ) -> c_int,
    >,
    pub poll: Option<
        unsafe extern "C" fn(
            *const c_char,
            *mut fuse_file_info,
            *mut fuse_pollhandle,
            *mut c_uint,
        ) -> c_int,
    >,
    pub write_buf: Option<
        unsafe extern "C" fn(
            *const c_char,
            *mut fuse_bufvec,
            off_t,
            *mut fuse_file_info,
        ) -> c_int,
    >,
    pub read_buf: Option<
        unsafe extern "C" fn(
            *const c_char,
            *mut *mut fuse_bufvec,
            size_t,
            off_t,
            *mut fuse_file_info,
        ) -> c_int,
    >,
    pub flock:
        Option<unsafe extern "C" fn(*const c_char, *mut fuse_file_info, c_int) -> c_int>,
    pub fallocate: Option<
        unsafe extern "C" fn(*const c_char, c_int, off_t, off_t, *mut fuse_file_info) -> c_int,
    >,
    pub copy_file_range: Option<
        unsafe extern "C" fn(
            *const c_char,
            *mut fuse_file_info,
            off_t,
            *const c_char,
            *mut fuse_file_info,
            off_t,
            size_t,
            c_int,
        ) -> ssize_t,
    >,
    pub lseek: Option<
        unsafe extern "C" fn(*const c_char, off_t, c_int, *mut fuse_file_info) -> off_t,
    >,
}

// Safety: fuse_operations is just a bag of function pointers — Send+Sync is fine.
unsafe impl Sync for fuse_operations {}
unsafe impl Send for fuse_operations {}

// ---------------------------------------------------------------------------
// Linked functions
// ---------------------------------------------------------------------------

#[link(name = "fuse3")]
extern "C" {
    /// Versioned symbol: `fuse_new_31` (FUSE 3.1+ without versioned symbols).
    /// Takes (args, ops, op_size, user_data) — same as the `fuse_new` macro.
    #[link_name = "fuse_new_31"]
    pub fn fuse_new(
        args: *mut fuse_args,
        op: *const fuse_operations,
        op_size: size_t,
        private_data: *mut c_void,
    ) -> *mut fuse;

    pub fn fuse_mount(f: *mut fuse, mountpoint: *const c_char) -> c_int;
    pub fn fuse_unmount(f: *mut fuse);
    pub fn fuse_destroy(f: *mut fuse);
    pub fn fuse_exit(f: *mut fuse);
    pub fn fuse_get_session(f: *mut fuse) -> *mut fuse_session;

    pub fn fuse_set_signal_handlers(se: *mut fuse_session) -> c_int;
    pub fn fuse_remove_signal_handlers(se: *mut fuse_session);

    pub fn fuse_daemonize(foreground: c_int) -> c_int;

    // fuse_loop_mt: we use the v3.12 opaque-config API
    #[link_name = "fuse_loop_mt_32"]
    pub fn fuse_loop_mt(f: *mut fuse, config: *mut fuse_loop_config) -> c_int;

    pub fn fuse_loop_cfg_create() -> *mut fuse_loop_config;
    pub fn fuse_loop_cfg_destroy(config: *mut fuse_loop_config);
    pub fn fuse_loop_cfg_set_idle_threads(config: *mut fuse_loop_config, value: c_uint);
    pub fn fuse_loop_cfg_set_clone_fd(config: *mut fuse_loop_config, value: c_uint);
}
