package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"lukechampine.com/blake3"
)

func computeBlake3(backingPath string) (string, error) {
	f, err := os.Open(backingPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		return "", err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)

	h := blake3.New(32, nil)
	buf := make([]byte, 65536)
	for {
		n, err := io.ReadFull(f, buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeSumFile(path string, hexDigest string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "%s\n", hexDigest)
	if err != nil {
		return err
	}
	return f.Sync()
}

func readSumFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	buf := make([]byte, 128)
	n, err := io.ReadFull(f, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return "", err
	}
	s := string(buf[:n])
	s = strings.TrimRight(s, "\n\r ")
	return s, nil
}

func fsyncDir(dirPath string) {
	f, err := os.Open(dirPath)
	if err != nil {
		return
	}
	defer f.Close()
	f.Sync()
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, 0755)
	return err
}

func removeEmptyParentDirs(path string, stopAt string) {
	dir := filepath.Dir(path)
	for len(dir) > len(stopAt) {
		err := os.Remove(dir)
		if err != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

func copyFileWithSync(src, dst string) error {
	sf, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sf.Close()

	tmpPath := dst + ".tmp"
	df, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	buf := make([]byte, 65536)
	_, err = io.CopyBuffer(df, sf, buf)
	if err != nil {
		df.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := df.Sync(); err != nil {
		df.Close()
		os.Remove(tmpPath)
		return err
	}
	df.Close()

	if err := os.Rename(tmpPath, dst); err != nil {
		os.Remove(tmpPath)
		return err
	}
	fsyncDir(filepath.Dir(dst))
	return nil
}
