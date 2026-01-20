//go:build linux

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
	f, err := os.OpenFile(path, flag|syscall.O_DIRECT, perm)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		f.Close()
		return nil, err
	}
	return newAlignedFile(f, flag), nil
}
