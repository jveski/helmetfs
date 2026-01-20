//go:build darwin

package main

import (
	"os"
	"syscall"
)

// directFile is an interface that abstracts both regular os.File and alignedFile
// for platform-specific direct I/O.
type directFile interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Seek(offset int64, whence int) (int64, error)
	Sync() error
	Stat() (os.FileInfo, error)
	Close() error
}

func openDirect(path string, flag int, perm os.FileMode) (directFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_NOCACHE, 1)
	if errno != 0 {
		f.Close()
		return nil, errno
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
