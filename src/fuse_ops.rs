//! FUSE operation callbacks implemented via the `fuser` crate's `Filesystem`
//! trait.
//!
//! `HelmetFs` is a passthrough filesystem that maps FUSE inode numbers to
//! paths in a backing directory.  An inode-to-path table is maintained so that
//! the low-level (inode-based) `fuser` API can be bridged to the path-based
//! operations on the backing store.
//!
//! File handle encoding:
//!   bit 63       = write flag (1 = opened for writing)
//!   bits 0..62   = file descriptor

use std::collections::HashMap;
use std::ffi::{CString, OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, INodeNo, MountOption, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr,
    Request,
};

use crate::helpers;
use crate::state::{self, checksum_and_enqueue, FsState};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TTL: Duration = Duration::from_secs(1);
const WRITE_FLAG: u64 = 1 << 63;

// ---------------------------------------------------------------------------
// File handle helpers
// ---------------------------------------------------------------------------

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
// Inode table
// ---------------------------------------------------------------------------

/// Bidirectional mapping between inode numbers and relative paths.
///
/// For a passthrough filesystem we use the real inode numbers from the
/// backing filesystem.  `lookup` populates the map; `forget` could be used
/// to clean it up (we keep entries indefinitely since they are cheap).
struct InodeTable {
    /// inode -> relative path (empty string = root)
    ino_to_path: HashMap<u64, String>,
    /// relative path -> inode
    path_to_ino: HashMap<String, u64>,
}

impl InodeTable {
    fn new() -> Self {
        Self {
            ino_to_path: HashMap::new(),
            path_to_ino: HashMap::new(),
        }
    }

    fn insert(&mut self, ino: u64, rel: String) {
        self.ino_to_path.insert(ino, rel.clone());
        self.path_to_ino.insert(rel, ino);
    }

    fn get_path(&self, ino: u64) -> Option<&str> {
        self.ino_to_path.get(&ino).map(|s| s.as_str())
    }

    fn get_ino(&self, rel: &str) -> Option<u64> {
        self.path_to_ino.get(rel).copied()
    }

    fn remove_path(&mut self, rel: &str) {
        if let Some(ino) = self.path_to_ino.remove(rel) {
            self.ino_to_path.remove(&ino);
        }
    }

    fn rename(&mut self, from: &str, to: &str) {
        if let Some(ino) = self.path_to_ino.remove(from) {
            self.ino_to_path.insert(ino, to.to_string());
            self.path_to_ino.insert(to.to_string(), ino);
        }
    }
}

// ---------------------------------------------------------------------------
// stat helpers
// ---------------------------------------------------------------------------

fn filetype_from_mode(mode: u32) -> FileType {
    let fmt = mode & libc::S_IFMT;
    match fmt {
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFREG => FileType::RegularFile,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFIFO => FileType::NamedPipe,
        libc::S_IFSOCK => FileType::Socket,
        _ => FileType::RegularFile,
    }
}

fn system_time_from_ts(sec: i64, nsec: i64) -> SystemTime {
    if sec >= 0 {
        UNIX_EPOCH + Duration::new(sec as u64, nsec as u32)
    } else {
        UNIX_EPOCH - Duration::new((-sec) as u64, nsec as u32)
    }
}

fn stat_to_file_attr(st: &libc::stat) -> FileAttr {
    FileAttr {
        ino: INodeNo(st.st_ino),
        size: st.st_size as u64,
        blocks: st.st_blocks as u64,
        atime: system_time_from_ts(st.st_atime, st.st_atime_nsec),
        mtime: system_time_from_ts(st.st_mtime, st.st_mtime_nsec),
        ctime: system_time_from_ts(st.st_ctime, st.st_ctime_nsec),
        crtime: UNIX_EPOCH,
        kind: filetype_from_mode(st.st_mode),
        perm: (st.st_mode & 0o7777) as u16,
        nlink: st.st_nlink as u32,
        uid: st.st_uid,
        gid: st.st_gid,
        rdev: st.st_rdev as u32,
        blksize: st.st_blksize as u32,
        flags: 0,
    }
}

/// lstat a backing-directory path, returning the raw libc::stat.
fn lstat_backing(state: &FsState, rel: &str) -> Result<libc::stat, libc::c_int> {
    let abs = state.backing_path(rel);
    let c_path =
        CString::new(abs.as_os_str().as_bytes()).map_err(|_| libc::EINVAL)?;
    unsafe {
        let mut st: libc::stat = std::mem::zeroed();
        if libc::lstat(c_path.as_ptr(), &mut st) == -1 {
            Err(*libc::__errno_location())
        } else {
            Ok(st)
        }
    }
}

/// Like lstat_backing but uses fstat on a file descriptor.
fn fstat_fd(fd: i32) -> Result<libc::stat, libc::c_int> {
    unsafe {
        let mut st: libc::stat = std::mem::zeroed();
        if libc::fstat(fd, &mut st) == -1 {
            Err(*libc::__errno_location())
        } else {
            Ok(st)
        }
    }
}

fn neg_errno() -> libc::c_int {
    unsafe { *libc::__errno_location() }
}

/// Build a CString from a backing-directory relative path.
fn backing_cpath(state: &FsState, rel: &str) -> CString {
    let abs = state.backing_path(rel);
    CString::new(abs.as_os_str().as_bytes()).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// HelmetFs
// ---------------------------------------------------------------------------

/// The main FUSE filesystem struct.
pub struct HelmetFs {
    state: Arc<FsState>,
    inodes: RwLock<InodeTable>,
}

impl HelmetFs {
    pub fn new(state: Arc<FsState>) -> Self {
        let mut table = InodeTable::new();
        // Seed root inode by statting the backing directory.
        let c_path =
            CString::new(state.backing_dir.as_os_str().as_bytes()).unwrap();
        let root_ino = unsafe {
            let mut st: libc::stat = std::mem::zeroed();
            if libc::lstat(c_path.as_ptr(), &mut st) == 0 {
                st.st_ino
            } else {
                1 // fallback
            }
        };
        table.insert(root_ino, String::new());
        Self {
            state,
            inodes: RwLock::new(table),
        }
    }

    /// Resolve an inode to a relative path.  Returns None if unknown.
    fn inode_path(&self, ino: INodeNo) -> Option<String> {
        let table = self.inodes.read().unwrap();
        table.get_path(ino.0).map(|s| s.to_string())
    }

    /// Register (or update) an inode <-> path mapping.
    fn register_inode(&self, ino: u64, rel: String) {
        let mut table = self.inodes.write().unwrap();
        table.insert(ino, rel);
    }

    /// Build the child relative path from parent inode + child name.
    fn child_rel(&self, parent: INodeNo, name: &OsStr) -> Option<String> {
        let parent_rel = self.inode_path(parent)?;
        let name_str = name.to_string_lossy();
        if parent_rel.is_empty() {
            Some(name_str.to_string())
        } else {
            Some(format!("{}/{}", parent_rel, name_str))
        }
    }

    /// Perform a lookup: stat the file, register the inode, return FileAttr.
    fn do_lookup(&self, parent: INodeNo, name: &OsStr) -> Result<FileAttr, libc::c_int> {
        let rel = self.child_rel(parent, name).ok_or(libc::ENOENT)?;

        if helpers::is_hidden_path(&rel, &self.state.backing_dir) {
            return Err(libc::ENOENT);
        }

        let st = lstat_backing(&self.state, &rel)?;
        let attr = stat_to_file_attr(&st);
        self.register_inode(st.st_ino, rel);
        Ok(attr)
    }
}

// ---------------------------------------------------------------------------
// Filesystem trait implementation
// ---------------------------------------------------------------------------

impl Filesystem for HelmetFs {
    // -- init / destroy -----------------------------------------------------

    fn init(
        &mut self,
        _req: &Request,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        Ok(())
    }

    fn destroy(&mut self) {}

    // -- lookup --------------------------------------------------------------

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        match self.do_lookup(parent, name) {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(e) => reply.error(e),
        }
    }

    // -- getattr / setattr ---------------------------------------------------

    fn getattr(&self, _req: &Request, ino: INodeNo, fh: Option<fuser::FileHandle>, reply: ReplyAttr) {
        // If we have an open file handle, use fstat
        if let Some(fh) = fh {
            let fd = decode_fd(u64::from(fh));
            match fstat_fd(fd) {
                Ok(st) => {
                    reply.attr(&TTL, &stat_to_file_attr(&st));
                    return;
                }
                Err(e) => {
                    reply.error(e);
                    return;
                }
            }
        }

        let rel = match self.inode_path(ino) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        if helpers::is_hidden_path(&rel, &self.state.backing_dir) {
            reply.error(libc::ENOENT);
            return;
        }

        match lstat_backing(&self.state, &rel) {
            Ok(st) => reply.attr(&TTL, &stat_to_file_attr(&st)),
            Err(e) => reply.error(e),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<fuser::FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let rel = match self.inode_path(ino) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let fd = fh.map(|h| decode_fd(u64::from(h)));
        let abs = backing_cpath(&self.state, &rel);

        // chmod
        if let Some(mode) = mode {
            let ret = if let Some(fd) = fd {
                unsafe { libc::fchmod(fd, mode) }
            } else {
                unsafe { libc::chmod(abs.as_ptr(), mode) }
            };
            if ret == -1 {
                reply.error(neg_errno());
                return;
            }
            // chmod triggers re-replication
            let backing = self.state.backing_path(&rel);
            if backing.is_file() {
                self.state.mark_dirty(&rel);
                state::checksum_if_idle(&self.state, &rel);
            }
        }

        // chown
        if uid.is_some() || gid.is_some() {
            let u = uid.unwrap_or(u32::MAX);
            let g = gid.unwrap_or(u32::MAX);
            let ret = if let Some(fd) = fd {
                unsafe { libc::fchown(fd, u, g) }
            } else {
                unsafe { libc::lchown(abs.as_ptr(), u, g) }
            };
            if ret == -1 {
                reply.error(neg_errno());
                return;
            }
        }

        // truncate
        if let Some(size) = size {
            let ret = if let Some(fd) = fd {
                unsafe { libc::ftruncate(fd, size as libc::off_t) }
            } else {
                unsafe { libc::truncate(abs.as_ptr(), size as libc::off_t) }
            };
            if ret == -1 {
                reply.error(neg_errno());
                return;
            }
            self.state.mark_dirty(&rel);
            state::checksum_if_idle(&self.state, &rel);
        }

        // utimens
        if atime.is_some() || mtime.is_some() {
            let to_timespec = |t: Option<fuser::TimeOrNow>| -> libc::timespec {
                match t {
                    Some(fuser::TimeOrNow::SpecificTime(st)) => {
                        let d = st.duration_since(UNIX_EPOCH).unwrap_or_default();
                        libc::timespec {
                            tv_sec: d.as_secs() as libc::time_t,
                            tv_nsec: d.subsec_nanos() as libc::c_long,
                        }
                    }
                    Some(fuser::TimeOrNow::Now) => libc::timespec {
                        tv_sec: 0,
                        tv_nsec: libc::UTIME_NOW,
                    },
                    None => libc::timespec {
                        tv_sec: 0,
                        tv_nsec: libc::UTIME_OMIT,
                    },
                }
            };
            let times = [to_timespec(atime), to_timespec(mtime)];
            let ret = if let Some(fd) = fd {
                unsafe { libc::futimens(fd, times.as_ptr()) }
            } else {
                unsafe {
                    libc::utimensat(
                        libc::AT_FDCWD,
                        abs.as_ptr(),
                        times.as_ptr(),
                        libc::AT_SYMLINK_NOFOLLOW,
                    )
                }
            };
            if ret == -1 {
                reply.error(neg_errno());
                return;
            }
        }

        // Return updated attrs
        match lstat_backing(&self.state, &rel) {
            Ok(st) => reply.attr(&TTL, &stat_to_file_attr(&st)),
            Err(e) => reply.error(e),
        }
    }

    // -- readlink ------------------------------------------------------------

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let rel = match self.inode_path(ino) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let abs = backing_cpath(&self.state, &rel);
        let mut buf = vec![0u8; libc::PATH_MAX as usize];
        let len =
            unsafe { libc::readlink(abs.as_ptr(), buf.as_mut_ptr() as *mut _, buf.len()) };
        if len == -1 {
            reply.error(neg_errno());
        } else {
            reply.data(&buf[..len as usize]);
        }
    }

    // -- mknod ---------------------------------------------------------------

    fn mknod(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let rel = match self.child_rel(parent, name) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        if helpers::is_hidden_path(&rel, &self.state.backing_dir) {
            reply.error(libc::EACCES);
            return;
        }
        let abs = backing_cpath(&self.state, &rel);
        if unsafe { libc::mknod(abs.as_ptr(), mode, rdev as libc::dev_t) } == -1 {
            reply.error(neg_errno());
            return;
        }
        match lstat_backing(&self.state, &rel) {
            Ok(st) => {
                let attr = stat_to_file_attr(&st);
                self.register_inode(st.st_ino, rel);
                reply.entry(&TTL, &attr, 0);
            }
            Err(e) => reply.error(e),
        }
    }

    // -- mkdir ---------------------------------------------------------------

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let rel = match self.child_rel(parent, name) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        if helpers::is_hidden_path(&rel, &self.state.backing_dir) {
            reply.error(libc::EACCES);
            return;
        }
        let abs = backing_cpath(&self.state, &rel);
        if unsafe { libc::mkdir(abs.as_ptr(), mode) } == -1 {
            reply.error(neg_errno());
            return;
        }
        match lstat_backing(&self.state, &rel) {
            Ok(st) => {
                let attr = stat_to_file_attr(&st);
                self.register_inode(st.st_ino, rel);
                reply.entry(&TTL, &attr, 0);
            }
            Err(e) => reply.error(e),
        }
    }

    // -- unlink --------------------------------------------------------------

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let rel = match self.child_rel(parent, name) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        if helpers::is_hidden_path(&rel, &self.state.backing_dir) {
            reply.error(libc::ENOENT);
            return;
        }

        let abs = backing_cpath(&self.state, &rel);
        if unsafe { libc::unlink(abs.as_ptr()) } == -1 {
            reply.error(neg_errno());
            return;
        }

        // Remove .sum sidecar
        let sum = helpers::sum_path_for(&self.state.backing_path(&rel));
        let _ = std::fs::remove_file(&sum);

        // Clean up path state and inode table
        self.state.remove_path_state(&rel);
        {
            let mut table = self.inodes.write().unwrap();
            table.remove_path(&rel);
        }

        // Enqueue delete to replica
        self.state.repl_log.enqueue_delete(&rel);

        reply.ok();
    }

    // -- rmdir ---------------------------------------------------------------

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let rel = match self.child_rel(parent, name) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let abs = backing_cpath(&self.state, &rel);
        if unsafe { libc::rmdir(abs.as_ptr()) } == -1 {
            reply.error(neg_errno());
            return;
        }
        {
            let mut table = self.inodes.write().unwrap();
            table.remove_path(&rel);
        }
        reply.ok();
    }

    // -- symlink -------------------------------------------------------------

    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let rel = match self.child_rel(parent, link_name) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        if helpers::is_hidden_path(&rel, &self.state.backing_dir) {
            reply.error(libc::EACCES);
            return;
        }

        let abs = backing_cpath(&self.state, &rel);
        let c_target = match CString::new(target.as_os_str().as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        if unsafe { libc::symlink(c_target.as_ptr(), abs.as_ptr()) } == -1 {
            reply.error(neg_errno());
            return;
        }

        // Enqueue symlink for replication
        self.state.repl_log.enqueue_put(&rel);

        match lstat_backing(&self.state, &rel) {
            Ok(st) => {
                let attr = stat_to_file_attr(&st);
                self.register_inode(st.st_ino, rel);
                reply.entry(&TTL, &attr, 0);
            }
            Err(e) => reply.error(e),
        }
    }

    // -- rename --------------------------------------------------------------

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let rel_from = match self.child_rel(parent, name) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let rel_to = match self.child_rel(newparent, newname) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let abs_from = backing_cpath(&self.state, &rel_from);
        let abs_to = backing_cpath(&self.state, &rel_to);

        if unsafe { libc::rename(abs_from.as_ptr(), abs_to.as_ptr()) } == -1 {
            reply.error(neg_errno());
            return;
        }

        // Move .sum sidecar if it exists
        let sum_from = helpers::sum_path_for(&self.state.backing_path(&rel_from));
        let sum_to = helpers::sum_path_for(&self.state.backing_path(&rel_to));
        if sum_from.exists() {
            let _ = std::fs::rename(&sum_from, &sum_to);
        }

        // Transfer path state
        {
            let mut map = self.state.path_state.write().unwrap();
            if let Some(info) = map.remove(&rel_from) {
                map.insert(rel_to.clone(), info);
            }
        }

        // Update inode table
        {
            let mut table = self.inodes.write().unwrap();
            table.rename(&rel_from, &rel_to);
        }

        // Enqueue delete for old path, put for new path
        self.state.repl_log.enqueue_delete(&rel_from);

        if sum_to.exists() {
            self.state.repl_log.enqueue_put(&rel_to);
        } else {
            let backing_to = self.state.backing_path(&rel_to);
            if backing_to.is_file() {
                checksum_and_enqueue(&self.state, &rel_to);
            }
        }

        reply.ok();
    }

    // -- link (unsupported) --------------------------------------------------

    fn link(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _newparent: INodeNo,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(libc::ENOTSUP);
    }

    // -- open ----------------------------------------------------------------

    fn open(&self, _req: &Request, ino: INodeNo, flags: i32, reply: ReplyOpen) {
        let rel = match self.inode_path(ino) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        if helpers::is_hidden_path(&rel, &self.state.backing_dir) {
            reply.error(libc::ENOENT);
            return;
        }

        let abs = backing_cpath(&self.state, &rel);
        let fd = unsafe { libc::open(abs.as_ptr(), flags) };
        if fd == -1 {
            reply.error(neg_errno());
            return;
        }

        let acc_mode = flags & libc::O_ACCMODE;
        let writing = acc_mode == libc::O_WRONLY
            || acc_mode == libc::O_RDWR
            || (flags & libc::O_APPEND) != 0;

        if writing {
            self.state.inc_write_ref(&rel);
        }

        reply.opened(encode_fh(fd, writing), 0);
    }

    // -- read ----------------------------------------------------------------

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let fd = decode_fd(fh);
        let mut buf = vec![0u8; size as usize];
        let ret =
            unsafe { libc::pread(fd, buf.as_mut_ptr() as *mut _, buf.len(), offset) };
        if ret == -1 {
            reply.error(neg_errno());
        } else {
            reply.data(&buf[..ret as usize]);
        }
    }

    // -- write ---------------------------------------------------------------

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let fd = decode_fd(fh);
        let ret = unsafe {
            libc::pwrite(fd, data.as_ptr() as *const _, data.len(), offset)
        };
        if ret == -1 {
            reply.error(neg_errno());
            return;
        }

        if let Some(rel) = self.inode_path(ino) {
            self.state.mark_dirty(&rel);
        }

        reply.written(ret as u32);
    }

    // -- statfs --------------------------------------------------------------

    fn statfs(&self, _req: &Request, ino: INodeNo, reply: ReplyStatfs) {
        let rel = self.inode_path(ino).unwrap_or_default();
        let abs = backing_cpath(&self.state, &rel);
        unsafe {
            let mut stbuf: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(abs.as_ptr(), &mut stbuf) == -1 {
                reply.error(neg_errno());
                return;
            }
            reply.statfs(
                stbuf.f_blocks,
                stbuf.f_bfree,
                stbuf.f_bavail,
                stbuf.f_files,
                stbuf.f_ffree,
                stbuf.f_bsize as u32,
                stbuf.f_namemax as u32,
                stbuf.f_frsize as u32,
            );
        }
    }

    // -- flush ---------------------------------------------------------------

    fn flush(&self, _req: &Request, _ino: INodeNo, fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        let fd = decode_fd(fh);
        let dup_fd = unsafe { libc::dup(fd) };
        if dup_fd == -1 {
            reply.error(neg_errno());
            return;
        }
        if unsafe { libc::close(dup_fd) } == -1 {
            reply.error(neg_errno());
            return;
        }
        reply.ok();
    }

    // -- release -------------------------------------------------------------

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let fd = decode_fd(fh);
        unsafe {
            libc::close(fd);
        }

        if is_write_fh(fh) {
            if let Some(rel) = self.inode_path(ino) {
                self.state.dec_write_ref(&rel);

                let info = self.state.get_path_info(&rel);
                if let Some(info) = info {
                    if info.write_ref == 0 && info.dirty {
                        checksum_and_enqueue(&self.state, &rel);
                    }
                }
            }
        }

        reply.ok();
    }

    // -- fsync ---------------------------------------------------------------

    fn fsync(&self, _req: &Request, _ino: INodeNo, fh: u64, datasync: bool, reply: ReplyEmpty) {
        let fd = decode_fd(fh);
        let ret = if datasync {
            unsafe { libc::fdatasync(fd) }
        } else {
            unsafe { libc::fsync(fd) }
        };
        if ret == -1 {
            reply.error(neg_errno());
        } else {
            reply.ok();
        }
    }

    // -- opendir -------------------------------------------------------------

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: i32, reply: ReplyOpen) {
        let rel = match self.inode_path(ino) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        if helpers::is_hidden_path(&rel, &self.state.backing_dir) {
            reply.error(libc::ENOENT);
            return;
        }

        let abs = backing_cpath(&self.state, &rel);
        let dp = unsafe { libc::opendir(abs.as_ptr()) };
        if dp.is_null() {
            reply.error(neg_errno());
            return;
        }
        reply.opened(dp as u64, 0);
    }

    // -- readdir -------------------------------------------------------------

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let rel = match self.inode_path(ino) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let abs_dir = self.state.backing_path(&rel);

        let entries = match std::fs::read_dir(&abs_dir) {
            Ok(e) => e,
            Err(e) => {
                reply.error(helpers::io_error_to_errno(&e));
                return;
            }
        };

        // Collect all entries (including . and ..) with filtering
        let mut all_entries: Vec<(INodeNo, FileType, OsString)> = Vec::new();

        // Add . and ..
        // Use the directory's own inode for "."
        all_entries.push((ino, FileType::Directory, OsString::from(".")));
        // For ".." we use ino 1 (parent); the kernel handles this.
        all_entries.push((INodeNo(1), FileType::Directory, OsString::from("..")));

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
            if helpers::is_hidden_path(&entry_rel, &self.state.backing_dir) {
                continue;
            }

            // Get file type and inode
            let ft = match entry.file_type() {
                Ok(ft) => {
                    if ft.is_dir() {
                        FileType::Directory
                    } else if ft.is_symlink() {
                        FileType::Symlink
                    } else {
                        FileType::RegularFile
                    }
                }
                Err(_) => FileType::RegularFile,
            };

            // Get real inode from metadata
            let entry_ino = match entry.metadata() {
                Ok(m) => {
                    use std::os::unix::fs::MetadataExt;
                    let ino_val = m.ino();
                    // Register in inode table
                    self.register_inode(ino_val, entry_rel);
                    INodeNo(ino_val)
                }
                Err(_) => INodeNo(0),
            };

            all_entries.push((entry_ino, ft, name));
        }

        // Send entries starting from offset
        for (i, (entry_ino, ft, name)) in all_entries.iter().enumerate().skip(offset as usize) {
            // offset+1 means the next readdir call will start after this entry
            let full = reply.add(*entry_ino, (i + 1) as i64, *ft, name);
            if full {
                break;
            }
        }

        reply.ok();
    }

    // -- releasedir ----------------------------------------------------------

    fn releasedir(&self, _req: &Request, _ino: INodeNo, fh: u64, _flags: i32, reply: ReplyEmpty) {
        let dp = fh as *mut libc::DIR;
        if !dp.is_null() {
            unsafe {
                libc::closedir(dp);
            }
        }
        reply.ok();
    }

    // -- access --------------------------------------------------------------

    fn access(&self, _req: &Request, ino: INodeNo, mask: i32, reply: ReplyEmpty) {
        let rel = match self.inode_path(ino) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        if helpers::is_hidden_path(&rel, &self.state.backing_dir) {
            reply.error(libc::ENOENT);
            return;
        }

        let abs = backing_cpath(&self.state, &rel);
        if unsafe { libc::access(abs.as_ptr(), mask) } == -1 {
            reply.error(neg_errno());
        } else {
            reply.ok();
        }
    }

    // -- create --------------------------------------------------------------

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let rel = match self.child_rel(parent, name) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        if helpers::is_hidden_path(&rel, &self.state.backing_dir) {
            reply.error(libc::EACCES);
            return;
        }

        let abs = backing_cpath(&self.state, &rel);
        let fd = unsafe { libc::open(abs.as_ptr(), flags | libc::O_CREAT, mode) };
        if fd == -1 {
            reply.error(neg_errno());
            return;
        }

        self.state.inc_write_ref(&rel);
        self.state.mark_dirty(&rel);

        let fh = encode_fh(fd, true);

        match lstat_backing(&self.state, &rel) {
            Ok(st) => {
                let attr = stat_to_file_attr(&st);
                self.register_inode(st.st_ino, rel);
                reply.created(&TTL, &attr, 0, fh, 0);
            }
            Err(e) => {
                // Clean up fd on error
                unsafe {
                    libc::close(fd);
                }
                reply.error(e);
            }
        }
    }

    // -- xattr ---------------------------------------------------------------

    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        let rel = match self.inode_path(ino) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let abs = backing_cpath(&self.state, &rel);
        let c_name = match CString::new(name.as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        if unsafe {
            libc::setxattr(
                abs.as_ptr(),
                c_name.as_ptr(),
                value.as_ptr() as *const _,
                value.len(),
                flags,
            )
        } == -1
        {
            reply.error(neg_errno());
        } else {
            reply.ok();
        }
    }

    fn getxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let rel = match self.inode_path(ino) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let abs = backing_cpath(&self.state, &rel);
        let c_name = match CString::new(name.as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        if size == 0 {
            // Return the size of the value
            let ret = unsafe {
                libc::getxattr(
                    abs.as_ptr(),
                    c_name.as_ptr(),
                    std::ptr::null_mut(),
                    0,
                )
            };
            if ret == -1 {
                reply.error(neg_errno());
            } else {
                reply.size(ret as u32);
            }
        } else {
            let mut buf = vec![0u8; size as usize];
            let ret = unsafe {
                libc::getxattr(
                    abs.as_ptr(),
                    c_name.as_ptr(),
                    buf.as_mut_ptr() as *mut _,
                    buf.len(),
                )
            };
            if ret == -1 {
                reply.error(neg_errno());
            } else {
                reply.data(&buf[..ret as usize]);
            }
        }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let rel = match self.inode_path(ino) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let abs = backing_cpath(&self.state, &rel);

        if size == 0 {
            let ret = unsafe { libc::listxattr(abs.as_ptr(), std::ptr::null_mut(), 0) };
            if ret == -1 {
                reply.error(neg_errno());
            } else {
                reply.size(ret as u32);
            }
        } else {
            let mut buf = vec![0u8; size as usize];
            let ret = unsafe {
                libc::listxattr(abs.as_ptr(), buf.as_mut_ptr() as *mut _, buf.len())
            };
            if ret == -1 {
                reply.error(neg_errno());
            } else {
                reply.data(&buf[..ret as usize]);
            }
        }
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let rel = match self.inode_path(ino) {
            Some(r) => r,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let abs = backing_cpath(&self.state, &rel);
        let c_name = match CString::new(name.as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        if unsafe { libc::removexattr(abs.as_ptr(), c_name.as_ptr()) } == -1 {
            reply.error(neg_errno());
        } else {
            reply.ok();
        }
    }
}
