package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

// HelmetRoot holds the root metadata for the passthrough filesystem.
type HelmetRoot struct {
	Path  string
	Dev   uint64
	State *FsState
}

func (r *HelmetRoot) idFromStat(st *syscall.Stat_t) fs.StableAttr {
	swapped := (uint64(st.Dev) << 32) | (uint64(st.Dev) >> 32)
	swappedRoot := (r.Dev << 32) | (r.Dev >> 32)
	return fs.StableAttr{
		Mode: uint32(st.Mode),
		Gen:  1,
		Ino:  (swapped ^ swappedRoot) ^ st.Ino,
	}
}

// NewHelmetRoot creates the root FUSE node for the given backing directory.
func NewHelmetRoot(backingDir string, state *FsState) (fs.InodeEmbedder, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(backingDir, &st); err != nil {
		return nil, err
	}
	root := &HelmetRoot{
		Path:  backingDir,
		Dev:   uint64(st.Dev),
		State: state,
	}
	return &HelmetNode{RootData: root}, nil
}

// HelmetNode is a FUSE inode that passes operations through to the backing directory.
type HelmetNode struct {
	fs.Inode
	RootData *HelmetRoot
}

func (n *HelmetNode) relPath() string     { return n.Path(n.Root()) }
func (n *HelmetNode) backingPath() string { return filepath.Join(n.RootData.Path, n.relPath()) }
func (n *HelmetNode) state() *FsState     { return n.RootData.State }

func (n *HelmetNode) newChild(ctx context.Context, name string, st *syscall.Stat_t) *fs.Inode {
	child := &HelmetNode{RootData: n.RootData}
	return n.NewInode(ctx, child, n.RootData.idFromStat(st))
}

// isHiddenPath returns true for paths that should be invisible through FUSE:
// the .helmetfs metadata directory and .sum sidecar files.
func isHiddenPath(backingDir, relPath string) bool {
	if strings.HasPrefix(relPath, ".helmetfs") {
		return true
	}
	if strings.HasSuffix(relPath, ".sum") {
		dataPath := filepath.Join(backingDir, strings.TrimSuffix(relPath, ".sum"))
		var st syscall.Stat_t
		return syscall.Lstat(dataPath, &st) == nil
	}
	return false
}

// Compile-time interface assertions for HelmetNode.
var (
	_ fs.NodeLookuper   = (*HelmetNode)(nil)
	_ fs.NodeGetattrer  = (*HelmetNode)(nil)
	_ fs.NodeReaddirer  = (*HelmetNode)(nil)
	_ fs.NodeOpener     = (*HelmetNode)(nil)
	_ fs.NodeCreater    = (*HelmetNode)(nil)
	_ fs.NodeUnlinker   = (*HelmetNode)(nil)
	_ fs.NodeRenamer    = (*HelmetNode)(nil)
	_ fs.NodeMkdirer    = (*HelmetNode)(nil)
	_ fs.NodeRmdirer    = (*HelmetNode)(nil)
	_ fs.NodeSymlinker  = (*HelmetNode)(nil)
	_ fs.NodeReadlinker = (*HelmetNode)(nil)
	_ fs.NodeSetattrer  = (*HelmetNode)(nil)
	_ fs.NodeStatfser   = (*HelmetNode)(nil)
	_ fs.NodeAccesser   = (*HelmetNode)(nil)
)

func (n *HelmetNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	rel := filepath.Join(n.relPath(), name)
	if rel != "" && isHiddenPath(n.RootData.Path, rel) {
		return nil, syscall.ENOENT
	}
	p := filepath.Join(n.backingPath(), name)
	var st syscall.Stat_t
	if err := syscall.Lstat(p, &st); err != nil {
		return nil, fs.ToErrno(err)
	}
	out.Attr.FromStat(&st)
	return n.newChild(ctx, name, &st), 0
}

func (n *HelmetNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	rel := n.relPath()
	if rel != "" && isHiddenPath(n.RootData.Path, rel) {
		return syscall.ENOENT
	}
	p := n.backingPath()
	var st syscall.Stat_t
	var err error
	if &n.Inode == n.Root() {
		err = syscall.Stat(p, &st)
	} else {
		err = syscall.Lstat(p, &st)
	}
	if err != nil {
		return fs.ToErrno(err)
	}
	out.FromStat(&st)
	return 0
}

func (n *HelmetNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := os.ReadDir(n.backingPath())
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	parentRel := n.relPath()
	result := []fuse.DirEntry{
		{Name: ".", Mode: syscall.S_IFDIR},
		{Name: "..", Mode: syscall.S_IFDIR},
	}
	for _, e := range entries {
		childRel := filepath.Join(parentRel, e.Name())
		if isHiddenPath(n.RootData.Path, childRel) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		stat := info.Sys().(*syscall.Stat_t)
		result = append(result, fuse.DirEntry{
			Name: e.Name(),
			Mode: uint32(stat.Mode),
			Ino:  stat.Ino,
		})
	}
	return fs.NewListDirStream(result), 0
}

func (n *HelmetNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	rel := n.relPath()
	if rel != "" && isHiddenPath(n.RootData.Path, rel) {
		return nil, 0, syscall.ENOENT
	}
	fd, err := syscall.Open(n.backingPath(), int(flags)&^syscall.O_APPEND, 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	forWrite := isWriteOpen(flags)
	if forWrite {
		n.state().pathState.IncWriteRef(rel)
	}
	if flags&syscall.O_TRUNC != 0 {
		n.state().pathState.SetDirty(rel)
	}
	fh := &helmetFile{fd: fd, forWrite: forWrite, relPath: rel, state: n.state()}
	return fh, 0, 0
}

func (n *HelmetNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	rel := filepath.Join(n.relPath(), name)
	if rel != "" && isHiddenPath(n.RootData.Path, rel) {
		return nil, nil, 0, syscall.ENOENT
	}
	p := filepath.Join(n.backingPath(), name)
	fd, err := syscall.Open(p, int(flags)&^syscall.O_APPEND|syscall.O_CREAT, mode)
	if err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		syscall.Close(fd)
		return nil, nil, 0, fs.ToErrno(err)
	}
	out.FromStat(&st)
	n.state().pathState.IncWriteRef(rel)
	fh := &helmetFile{fd: fd, forWrite: true, relPath: rel, state: n.state()}
	return n.newChild(ctx, name, &st), fh, 0, 0
}

func (n *HelmetNode) Unlink(ctx context.Context, name string) syscall.Errno {
	rel := filepath.Join(n.relPath(), name)
	if rel != "" && isHiddenPath(n.RootData.Path, rel) {
		return syscall.ENOENT
	}
	p := filepath.Join(n.backingPath(), name)
	if err := syscall.Unlink(p); err != nil {
		return fs.ToErrno(err)
	}
	os.Remove(p + ".sum")
	if rel != "" {
		n.state().replLog.Enqueue(ReplDelete, rel)
	}
	n.state().pathState.Remove(rel)
	return 0
}

func (n *HelmetNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	np, ok := newParent.(*HelmetNode)
	if !ok {
		return syscall.EXDEV
	}
	relFrom := filepath.Join(n.relPath(), name)
	relTo := filepath.Join(np.relPath(), newName)
	if (relFrom != "" && isHiddenPath(n.RootData.Path, relFrom)) || (relTo != "" && isHiddenPath(n.RootData.Path, relTo)) {
		return syscall.ENOENT
	}

	switch {
	case flags == 0:
		// Standard rename.
	case flags == unix.RENAME_NOREPLACE:
		// Allowed.
	case flags == unix.RENAME_EXCHANGE:
		return syscall.EOPNOTSUPP
	default:
		return syscall.EINVAL
	}

	p1 := filepath.Join(n.backingPath(), name)
	p2 := filepath.Join(np.backingPath(), newName)

	if flags == unix.RENAME_NOREPLACE {
		fd1, err := syscall.Open(n.backingPath(), syscall.O_DIRECTORY, 0)
		if err != nil {
			return fs.ToErrno(err)
		}
		defer syscall.Close(fd1)
		fd2, err := syscall.Open(np.backingPath(), syscall.O_DIRECTORY, 0)
		if err != nil {
			return fs.ToErrno(err)
		}
		defer syscall.Close(fd2)
		if err := unix.Renameat2(fd1, name, fd2, newName, unix.RENAME_NOREPLACE); err != nil {
			return fs.ToErrno(err)
		}
	} else {
		if err := syscall.Rename(p1, p2); err != nil {
			return fs.ToErrno(err)
		}
	}

	os.Rename(p1+".sum", p2+".sum")
	if relFrom != "" && relTo != "" {
		n.state().replLog.Enqueue(ReplDelete, relFrom)
		n.state().replLog.Enqueue(ReplPut, relTo)
	}
	n.state().pathState.Remove(relFrom)
	return 0
}

func (n *HelmetNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	rel := filepath.Join(n.relPath(), name)
	if rel != "" && isHiddenPath(n.RootData.Path, rel) {
		return nil, syscall.ENOENT
	}
	p := filepath.Join(n.backingPath(), name)
	if err := os.Mkdir(p, os.FileMode(mode)); err != nil {
		return nil, fs.ToErrno(err)
	}
	syscall.Chmod(p, mode)
	var st syscall.Stat_t
	if err := syscall.Lstat(p, &st); err != nil {
		return nil, fs.ToErrno(err)
	}
	out.Attr.FromStat(&st)
	if !n.state().noRemoteMkdir && rel != "" {
		replicaPath := filepath.Join(n.state().replicaDir, "files", rel)
		ensureParentDir(replicaPath)
		os.Mkdir(replicaPath, os.FileMode(mode))
	}
	return n.newChild(ctx, name, &st), 0
}

func (n *HelmetNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	rel := filepath.Join(n.relPath(), name)
	p := filepath.Join(n.backingPath(), name)
	if err := syscall.Rmdir(p); err != nil {
		return fs.ToErrno(err)
	}
	if !n.state().noRemoteMkdir && rel != "" {
		replicaPath := filepath.Join(n.state().replicaDir, "files", rel)
		os.Remove(replicaPath)
	}
	return 0
}

func (n *HelmetNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	rel := filepath.Join(n.relPath(), name)
	p := filepath.Join(n.backingPath(), name)
	if err := syscall.Symlink(target, p); err != nil {
		return nil, fs.ToErrno(err)
	}
	var st syscall.Stat_t
	if err := syscall.Lstat(p, &st); err != nil {
		return nil, fs.ToErrno(err)
	}
	out.Attr.FromStat(&st)
	if rel != "" {
		n.state().replLog.Enqueue(ReplPut, rel)
	}
	return n.newChild(ctx, name, &st), 0
}

func (n *HelmetNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	rel := n.relPath()
	if rel != "" && isHiddenPath(n.RootData.Path, rel) {
		return nil, syscall.ENOENT
	}
	target, err := os.Readlink(n.backingPath())
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	return []byte(target), 0
}

func (n *HelmetNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if fh, ok := f.(*helmetFile); ok && fh != nil {
		return fh.Setattr(ctx, in, out)
	}
	rel := n.relPath()
	p := n.backingPath()

	if mode, ok := in.GetMode(); ok {
		if err := syscall.Chmod(p, mode); err != nil {
			return syscall.EIO
		}
		if rel != "" {
			n.state().replLog.Enqueue(ReplPut, rel)
		}
	}

	uid, uok := in.GetUID()
	gid, gok := in.GetGID()
	if uok || gok {
		suid, sgid := -1, -1
		if uok {
			suid = int(uid)
		}
		if gok {
			sgid = int(gid)
		}
		if err := syscall.Lchown(p, suid, sgid); err != nil {
			return fs.ToErrno(err)
		}
		if rel != "" {
			n.state().replLog.Enqueue(ReplPut, rel)
		}
	}

	if err := setTimestamps(p, in); err != 0 {
		return err
	}
	if mok, aok := hasTimeAttrs(in); (mok || aok) && rel != "" {
		n.state().replLog.Enqueue(ReplPut, rel)
	}

	if sz, ok := in.GetSize(); ok {
		fd, err := syscall.Open(p, syscall.O_RDWR, 0)
		if err != nil {
			return syscall.ENOENT
		}
		truncErr := syscall.Ftruncate(fd, int64(sz))
		syscall.Close(fd)
		if truncErr != nil {
			return syscall.EIO
		}
		if rel != "" {
			n.state().pathState.SetDirty(rel)
			checksumAndEnqueue(n.state(), rel)
		}
	}

	var st syscall.Stat_t
	if err := syscall.Lstat(p, &st); err != nil {
		return fs.ToErrno(err)
	}
	out.FromStat(&st)
	return 0
}

func (n *HelmetNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	var s syscall.Statfs_t
	if err := syscall.Statfs(n.backingPath(), &s); err != nil {
		return syscall.EIO
	}
	out.FromStatfsT(&s)
	return 0
}

func (n *HelmetNode) Access(ctx context.Context, mask uint32) syscall.Errno {
	rel := n.relPath()
	if rel != "" && isHiddenPath(n.RootData.Path, rel) {
		return syscall.ENOENT
	}
	if err := syscall.Access(n.backingPath(), mask); err != nil {
		return fs.ToErrno(err)
	}
	return 0
}


// helmetFile is a FUSE file handle that wraps a raw file descriptor.
type helmetFile struct {
	fd       int
	forWrite bool
	relPath  string
	state    *FsState
}

// Compile-time interface assertions for helmetFile.
var (
	_ fs.FileHandle    = (*helmetFile)(nil)
	_ fs.FileReader    = (*helmetFile)(nil)
	_ fs.FileWriter    = (*helmetFile)(nil)
	_ fs.FileFsyncer   = (*helmetFile)(nil)
	_ fs.FileReleaser  = (*helmetFile)(nil)
	_ fs.FileGetattrer = (*helmetFile)(nil)
	_ fs.FileSetattrer = (*helmetFile)(nil)
	_ fs.FileFlusher   = (*helmetFile)(nil)
)

func (f *helmetFile) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := syscall.Pread(f.fd, dest, off)
	if err != nil {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (f *helmetFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, err := syscall.Pwrite(f.fd, data, off)
	if err != nil {
		return 0, syscall.EIO
	}
	if f.relPath != "" {
		f.state.pathState.SetDirty(f.relPath)
	}
	return uint32(n), 0
}

const fsyncDataOnly = 1

func (f *helmetFile) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	if flags&fsyncDataOnly != 0 {
		syscall.Fdatasync(f.fd)
	} else {
		syscall.Fsync(f.fd)
	}
	if f.relPath != "" && f.state.pathState.IsDirty(f.relPath) {
		if err := checksumAndEnqueueForced(f.state, f.relPath); err != nil {
			return syscall.EIO
		}
	}
	return 0
}

func (f *helmetFile) Release(ctx context.Context) syscall.Errno {
	if f.forWrite {
		f.state.pathState.DecWriteRef(f.relPath)
	}
	syscall.Close(f.fd)
	if f.relPath != "" && f.state.pathState.IsDirty(f.relPath) {
		if err := checksumAndEnqueue(f.state, f.relPath); err != nil {
			log.Printf("release checksum error for %s: %v", f.relPath, err)
		}
	}
	return 0
}

func (f *helmetFile) Getattr(ctx context.Context, out *fuse.AttrOut) syscall.Errno {
	var st syscall.Stat_t
	if err := syscall.Fstat(f.fd, &st); err != nil {
		return fs.ToErrno(err)
	}
	out.FromStat(&st)
	return 0
}

func (f *helmetFile) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if mode, ok := in.GetMode(); ok {
		if err := syscall.Fchmod(f.fd, mode); err != nil {
			return syscall.EIO
		}
	}
	uid, uok := in.GetUID()
	gid, gok := in.GetGID()
	if uok || gok {
		suid, sgid := -1, -1
		if uok {
			suid = int(uid)
		}
		if gok {
			sgid = int(gid)
		}
		if err := syscall.Fchown(f.fd, suid, sgid); err != nil {
			return fs.ToErrno(err)
		}
	}
	if sz, ok := in.GetSize(); ok {
		if err := syscall.Ftruncate(f.fd, int64(sz)); err != nil {
			return syscall.EIO
		}
		if f.relPath != "" {
			f.state.pathState.SetDirty(f.relPath)
			checksumAndEnqueue(f.state, f.relPath)
		}
	}

	if err := setTimestamps(filepath.Join(f.state.backingDir, f.relPath), in); err != 0 {
		return err
	}

	// Enqueue replication for metadata changes.
	if f.relPath != "" {
		_, modeSet := in.GetMode()
		mok, aok := hasTimeAttrs(in)
		if modeSet || uok || gok || mok || aok {
			f.state.replLog.Enqueue(ReplPut, f.relPath)
		}
	}
	return f.Getattr(ctx, out)
}

func (f *helmetFile) Flush(ctx context.Context) syscall.Errno {
	newFd, err := syscall.Dup(f.fd)
	if err != nil {
		return fs.ToErrno(err)
	}
	syscall.Close(newFd)
	return 0
}

// isWriteOpen returns true if the open flags indicate a write operation.
func isWriteOpen(flags uint32) bool {
	accMode := flags & syscall.O_ACCMODE
	return accMode == syscall.O_WRONLY || accMode == syscall.O_RDWR || flags&syscall.O_TRUNC != 0
}

// hasTimeAttrs returns whether mtime or atime is being set.
func hasTimeAttrs(in *fuse.SetAttrIn) (mok, aok bool) {
	_, mok = in.GetMTime()
	_, aok = in.GetATime()
	return
}

// setTimestamps applies atime/mtime changes to a path.
func setTimestamps(path string, in *fuse.SetAttrIn) syscall.Errno {
	mtime, mok := in.GetMTime()
	atime, aok := in.GetATime()
	if !mok && !aok {
		return 0
	}
	ta := unix.Timespec{Nsec: unix.UTIME_OMIT}
	tm := unix.Timespec{Nsec: unix.UTIME_OMIT}
	if aok {
		ta, _ = unix.TimeToTimespec(atime)
	}
	if mok {
		tm, _ = unix.TimeToTimespec(mtime)
	}
	if err := unix.UtimesNanoAt(unix.AT_FDCWD, path, []unix.Timespec{ta, tm}, 0); err != nil {
		return fs.ToErrno(err)
	}
	return 0
}

