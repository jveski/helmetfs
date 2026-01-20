package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"
)

type file struct {
	db        *sql.DB
	blobsDir  string
	path      string
	flag      int
	perm      os.FileMode
	version   int
	isDir     bool
	size      int64
	modTime   int64
	readFile  *os.File
	writeFile *os.File
	blobID    int64
	dirOffset int
	created   bool
	writeHash hash.Hash
}

func openFile(ctx context.Context, db *sql.DB, blobsDir, name string, flag int, perm os.FileMode) (*file, error) {
	var version int
	var blobID sql.NullInt64
	var isDir bool
	var size int64
	var mode int64
	var modTime int64
	var checksum sql.NullString
	var localWritten sql.NullBool
	var deleted bool
	err := db.QueryRowContext(ctx, `
		SELECT f.version, f.blob_id, f.is_dir, COALESCE(b.size, 0), f.mode, f.mod_time, b.checksum, b.local_written, f.deleted
		FROM files f
		LEFT JOIN blobs b ON f.blob_id = b.id
		WHERE f.path = ?
		ORDER BY f.version DESC
		LIMIT 1`, name).Scan(&version, &blobID, &isDir, &size, &mode, &modTime, &checksum, &localWritten, &deleted)
	exists := err == nil && !deleted
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if exists && flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
		return nil, os.ErrExist
	}
	if exists && isDir && (flag&(os.O_WRONLY|os.O_RDWR|os.O_TRUNC) != 0) {
		return nil, os.ErrInvalid
	}

	if exists {
		f := &file{
			db:       db,
			blobsDir: blobsDir,
			path:     name,
			flag:     flag,
			perm:     os.FileMode(mode),
			version:  version,
			isDir:    isDir,
			size:     size,
			modTime:  modTime,
		}
		if !isDir && flag&os.O_TRUNC == 0 && blobID.Valid {
			if !localWritten.Valid || !localWritten.Bool {
				return nil, fmt.Errorf("blob not available locally")
			}
			blobPath := blobFilePath(blobsDir, blobID.Int64)
			f.readFile, err = open(blobPath, os.O_RDONLY, 0)
			if err != nil {
				return nil, err
			}
			if checksum.String != "" {
				h := sha256.New()
				_, err = io.Copy(h, f.readFile)
				if err != nil {
					f.readFile.Close()
					return nil, err
				}
				actual := hex.EncodeToString(h.Sum(nil))
				if actual != checksum.String {
					f.readFile.Close()
					return nil, fmt.Errorf("checksum verification failed: expected=%s actual=%s", checksum.String, actual)
				}
				f.readFile.Seek(0, io.SeekStart)
			}
		}
		return f, nil
	}

	// Only create missing file if asked to
	if flag&os.O_CREATE == 0 {
		return nil, os.ErrNotExist
	}

	// Require that parent is an undeleted directory
	parent := path.Dir(name)
	if parent != "/" && parent != "." {
		var parentIsDir bool
		err := db.QueryRowContext(ctx, `SELECT is_dir FROM files WHERE path = ? AND deleted = 0 ORDER BY version DESC LIMIT 1`, parent).Scan(&parentIsDir)
		if err == sql.ErrNoRows {
			return nil, os.ErrNotExist
		}
		if err != nil {
			return nil, err
		}
		if !parentIsDir {
			return nil, os.ErrInvalid
		}
	}

	return &file{db: db, blobsDir: blobsDir, path: name, flag: flag, perm: perm, version: version, modTime: time.Now().Unix(), created: true}, nil
}

func (f *file) Read(p []byte) (int, error) {
	if f.isDir {
		return 0, os.ErrInvalid
	}
	if f.flag&os.O_WRONLY != 0 {
		return 0, os.ErrPermission
	}
	if f.readFile == nil {
		return 0, io.EOF
	}
	return f.readFile.Read(p)
}

func (f *file) Write(p []byte) (int, error) {
	if f.isDir {
		return 0, os.ErrInvalid // can't write dirs
	}
	if f.flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		return 0, os.ErrPermission // must have permission
	}

	// Write directly to the blob if it's already open
	if f.writeFile != nil {
		f.writeHash.Write(p)
		return f.writeFile.Write(p)
	}

	// Create a new blob
	now := time.Now().Unix()
	result, err := f.db.Exec(`INSERT INTO blobs (creation_time, modified_time, local_written) VALUES (?, ?, 0)`, now, now)
	if err != nil {
		return 0, err
	}
	f.blobID, err = result.LastInsertId()
	if err != nil {
		return 0, err
	}

	blobPath := blobFilePath(f.blobsDir, f.blobID)
	f.writeFile, err = open(blobPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return 0, err
	}
	f.writeHash = sha256.New()
	f.writeHash.Write(p)
	return f.writeFile.Write(p)
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	if f.isDir {
		var base int
		switch whence {
		case io.SeekStart:
			base = 0
		case io.SeekCurrent:
			base = f.dirOffset
		case io.SeekEnd:
			prefix := f.path
			if prefix != "/" {
				prefix += "/"
			}
			var count int
			err := f.db.QueryRow(`
				SELECT COUNT(*)
				FROM files f
				WHERE f.path GLOB ? AND f.path NOT GLOB ? AND f.deleted = 0
				AND f.version = (SELECT MAX(f2.version) FROM files f2 WHERE f2.path = f.path)`,
				prefix+"*", prefix+"*/*").Scan(&count)
			if err != nil {
				return 0, err
			}
			base = count
		}
		f.dirOffset = base + int(offset)
		return int64(f.dirOffset), nil
	}
	if f.writeFile != nil {
		return f.writeFile.Seek(offset, whence)
	}
	if f.readFile != nil {
		return f.readFile.Seek(offset, whence)
	}
	if offset == 0 && whence == io.SeekStart {
		return 0, nil
	}
	return 0, io.EOF
}

func (f *file) Close() error {
	if f.readFile != nil {
		f.readFile.Close()
	}
	if f.writeFile == nil {
		if f.created {
			now := time.Now().Unix()
			_, err := f.db.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir) VALUES (?, 1, ?, ?, ?, 0)`, now, f.path, f.perm, now)
			return err
		}
		return nil
	}
	stat, err := f.writeFile.Stat()
	f.writeFile.Sync()
	f.writeFile.Close()
	if err != nil {
		return err
	}

	streamChecksum := hex.EncodeToString(f.writeHash.Sum(nil))

	blobPath := blobFilePath(f.blobsDir, f.blobID)
	verifyFile, err := open(blobPath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	verifyHash := sha256.New()
	_, err = io.Copy(verifyHash, verifyFile)
	verifyFile.Close()
	if err != nil {
		return err
	}
	verifyChecksum := hex.EncodeToString(verifyHash.Sum(nil))

	if streamChecksum != verifyChecksum {
		return fmt.Errorf("checksum mismatch: stream=%s disk=%s", streamChecksum, verifyChecksum)
	}

	now := time.Now().Unix()
	tx, err := f.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var useBlobID int64
	err = tx.QueryRow(`SELECT id FROM blobs WHERE checksum = ? AND local_written = 1 AND local_deleting = 0`, streamChecksum).Scan(&useBlobID)
	if err == nil {
		// Reuse existing blob with same content
		_, err = tx.Exec(`DELETE FROM blobs WHERE id = ?`, f.blobID)
		if err != nil {
			return err
		}
		os.Remove(blobPath)
	} else if err == sql.ErrNoRows {
		// No existing blob - finalize our new blob
		useBlobID = f.blobID
		result, err := tx.Exec(`UPDATE blobs SET local_written = 1, size = ?, modified_time = ?, checksum = ? WHERE id = ? AND local_deleting = 0 AND local_deleted = 0`, stat.Size(), now, streamChecksum, f.blobID)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n == 0 {
			return fmt.Errorf("timeout - the client took too long to write the file")
		}
	} else {
		return err
	}

	_, err = tx.Exec(`INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, blob_id) VALUES (?, ?, ?, ?, ?, 0, ?)`, now, f.version+1, f.path, f.perm, now, useBlobID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (f *file) Readdir(count int) ([]os.FileInfo, error) {
	if !f.isDir {
		return nil, os.ErrInvalid
	}

	prefix := f.path
	if prefix != "/" {
		prefix += "/"
	}

	query := `
		SELECT f.path, COALESCE(b.size, 0), f.mode, f.mod_time, f.is_dir
		FROM files f
		LEFT JOIN blobs b ON f.blob_id = b.id
		WHERE f.path GLOB ? AND f.path NOT GLOB ? AND f.deleted = 0
		AND f.version = (SELECT MAX(f2.version) FROM files f2 WHERE f2.path = f.path)
		ORDER BY f.path`

	var rows *sql.Rows
	var err error
	if count <= 0 {
		rows, err = f.db.Query(query+" LIMIT -1 OFFSET ?", prefix+"*", prefix+"*/*", f.dirOffset)
	} else {
		rows, err = f.db.Query(query+" LIMIT ? OFFSET ?", prefix+"*", prefix+"*/*", count, f.dirOffset)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []os.FileInfo
	for rows.Next() {
		var filePath string
		fi := &fileInfo{}
		if err := rows.Scan(&filePath, &fi.size, &fi.mode, &fi.modTime, &fi.isDir); err != nil {
			return nil, err
		}
		fi.name = path.Base(filePath)
		result = append(result, fi)
	}

	f.dirOffset += len(result)

	if len(result) == 0 && count >= 0 {
		return nil, io.EOF
	}

	return result, nil
}

func (f *file) Stat() (os.FileInfo, error) {
	return &fileInfo{
		name:    path.Base(f.path),
		size:    f.size,
		mode:    f.perm,
		modTime: f.modTime,
		isDir:   f.isDir,
	}, nil
}

func open(path string, flag int, perm os.FileMode) (*os.File, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// fileInfo implements os.FileInfo
type fileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime int64
	isDir   bool
}

func (fi *fileInfo) Name() string       { return fi.name }
func (fi *fileInfo) Size() int64        { return fi.size }
func (fi *fileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi *fileInfo) ModTime() time.Time { return time.Unix(fi.modTime, 0) }
func (fi *fileInfo) IsDir() bool        { return fi.isDir }
func (fi *fileInfo) Sys() any           { return nil }

func blobFilePath(blobsDir string, blobID int64) string {
	h := fnv.New64a()
	binary.Write(h, binary.LittleEndian, blobID)
	hash := h.Sum64()
	return filepath.Join(blobsDir, fmt.Sprintf("%02x", hash&0xff), fmt.Sprintf("%014x", hash>>8))
}

func initBlobDirs(blobsDir string) error {
	for i := 0; i < 256; i++ {
		dir := filepath.Join(blobsDir, fmt.Sprintf("%02x", i))
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return nil
}
