//go:build linux

package main

import (
	"io"
	"os"
	"unsafe"
)

const alignment = 4096

// alignedBuffer allocates a byte slice with the given size, aligned to 4096 bytes.
// The returned slice's underlying array is aligned for O_DIRECT I/O.
func alignedBuffer(size int) []byte {
	// Allocate extra space to allow for alignment
	buf := make([]byte, size+alignment)
	offset := alignment - int(uintptr(unsafe.Pointer(&buf[0]))%alignment)
	if offset == alignment {
		offset = 0
	}
	return buf[offset : offset+size]
}

// alignedReader wraps an os.File opened with O_DIRECT and handles alignment requirements.
type alignedReader struct {
	f      *os.File
	buf    []byte
	bufPos int
	bufLen int
}

func newAlignedReader(f *os.File) *alignedReader {
	return &alignedReader{
		f:   f,
		buf: alignedBuffer(64 * 1024), // 64KB buffer
	}
}

func (r *alignedReader) Read(p []byte) (int, error) {
	// If we have buffered data, return from it
	if r.bufPos < r.bufLen {
		n := copy(p, r.buf[r.bufPos:r.bufLen])
		r.bufPos += n
		return n, nil
	}

	// Need to read more data
	n, err := r.f.Read(r.buf)
	if n > 0 {
		r.bufLen = n
		r.bufPos = 0
		copied := copy(p, r.buf[:n])
		r.bufPos = copied
		return copied, nil
	}
	return 0, err
}

func (r *alignedReader) Close() error {
	return r.f.Close()
}

// alignedWriter wraps an os.File opened with O_DIRECT and handles alignment requirements.
// It buffers writes and flushes in aligned chunks.
type alignedWriter struct {
	f      *os.File
	buf    []byte
	bufLen int
}

func newAlignedWriter(f *os.File) *alignedWriter {
	return &alignedWriter{
		f:   f,
		buf: alignedBuffer(64 * 1024), // 64KB buffer
	}
}

func (w *alignedWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		// Fill the buffer
		n := copy(w.buf[w.bufLen:], p)
		w.bufLen += n
		p = p[n:]
		written += n

		// Flush if buffer is full (aligned)
		if w.bufLen == len(w.buf) {
			if err := w.flushAligned(); err != nil {
				return written - len(p), err
			}
		}
	}
	return written, nil
}

// flushAligned writes full aligned blocks to the file
func (w *alignedWriter) flushAligned() error {
	if w.bufLen == 0 {
		return nil
	}
	// Write full aligned chunks
	alignedLen := (w.bufLen / alignment) * alignment
	if alignedLen > 0 {
		_, err := w.f.Write(w.buf[:alignedLen])
		if err != nil {
			return err
		}
		// Move remaining data to the beginning
		copy(w.buf, w.buf[alignedLen:w.bufLen])
		w.bufLen -= alignedLen
	}
	return nil
}

// Sync flushes any remaining data (padding if necessary) and syncs the file.
// The file will be truncated to the actual size after sync.
func (w *alignedWriter) Sync() error {
	// Flush any remaining aligned data first
	if err := w.flushAligned(); err != nil {
		return err
	}

	// If there's remaining unaligned data, we need to pad and write it
	if w.bufLen > 0 {
		// Pad to alignment boundary with zeros
		paddedLen := ((w.bufLen + alignment - 1) / alignment) * alignment
		for i := w.bufLen; i < paddedLen; i++ {
			w.buf[i] = 0
		}
		_, err := w.f.Write(w.buf[:paddedLen])
		if err != nil {
			return err
		}
		w.bufLen = 0
	}

	return w.f.Sync()
}

func (w *alignedWriter) Close() error {
	return w.f.Close()
}

func (w *alignedWriter) Stat() (os.FileInfo, error) {
	return w.f.Stat()
}

// alignedFile wraps an os.File for O_DIRECT I/O with alignment handling.
// It provides Read, Write, Seek, Sync, Stat and Close methods.
type alignedFile struct {
	f            *os.File
	reader       *alignedReader
	writer       *alignedWriter
	writeFlag    bool
	actualSize   int64 // Track actual bytes written (before padding)
	seekPos      int64 // Current logical position
	fileOpenedRO bool  // Opened read-only
}

func newAlignedFile(f *os.File, flag int) *alignedFile {
	af := &alignedFile{
		f:         f,
		writeFlag: flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE) != 0,
	}
	if af.writeFlag {
		af.writer = newAlignedWriter(f)
	} else {
		af.reader = newAlignedReader(f)
		af.fileOpenedRO = true
	}
	return af
}

func (af *alignedFile) Read(p []byte) (int, error) {
	if af.reader != nil {
		n, err := af.reader.Read(p)
		af.seekPos += int64(n)
		return n, err
	}
	return 0, io.EOF
}

func (af *alignedFile) Write(p []byte) (int, error) {
	if af.writer == nil {
		return 0, os.ErrPermission
	}
	n, err := af.writer.Write(p)
	af.actualSize += int64(n)
	af.seekPos += int64(n)
	return n, err
}

func (af *alignedFile) Seek(offset int64, whence int) (int64, error) {
	// For read-only files, we need to handle seeking
	if af.fileOpenedRO && af.reader != nil {
		// Reset reader buffer and seek underlying file
		af.reader.bufPos = 0
		af.reader.bufLen = 0
		pos, err := af.f.Seek(offset, whence)
		if err != nil {
			return 0, err
		}
		af.seekPos = pos
		return pos, nil
	}
	// For write files, track logical position
	switch whence {
	case io.SeekStart:
		af.seekPos = offset
	case io.SeekCurrent:
		af.seekPos += offset
	case io.SeekEnd:
		af.seekPos = af.actualSize + offset
	}
	return af.seekPos, nil
}

func (af *alignedFile) Sync() error {
	if af.writer != nil {
		if err := af.writer.Sync(); err != nil {
			return err
		}
		// Truncate file to actual size (remove padding)
		return af.f.Truncate(af.actualSize)
	}
	return nil
}

func (af *alignedFile) Stat() (os.FileInfo, error) {
	return af.f.Stat()
}

func (af *alignedFile) Close() error {
	if af.reader != nil {
		return af.reader.Close()
	}
	if af.writer != nil {
		return af.writer.Close()
	}
	return af.f.Close()
}

// ActualSize returns the actual number of bytes written (before alignment padding)
func (af *alignedFile) ActualSize() int64 {
	return af.actualSize
}
