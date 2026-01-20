//go:build linux

package main

import (
	"os"
	"syscall"
)

func openDirect(path string, flag int, perm os.FileMode) (*os.File, error) {
	openFlags := flag
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE) == 0 {
		openFlags |= syscall.O_DIRECT
	}
	f, err := os.OpenFile(path, openFlags, perm)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
