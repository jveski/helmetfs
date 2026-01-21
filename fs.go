package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path"
	"syscall"
	"time"

	"golang.org/x/net/webdav"
)

type FS struct {
	db       *sql.DB
	blobsDir string
}

func (fs *FS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	name = path.Clean(name)
	tx, err := fs.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Make sure the parent directory exists
	parent := path.Dir(name)
	if parent != "/" && parent != "." {
		var parentIsDir bool
		var parentDeleted bool
		err := tx.QueryRowContext(ctx, `SELECT is_dir, deleted FROM files WHERE path = ? ORDER BY version DESC LIMIT 1`, parent).Scan(&parentIsDir, &parentDeleted)
		if err == sql.ErrNoRows || parentDeleted {
			return os.ErrNotExist
		}
		if err != nil {
			return err
		}
		if !parentIsDir {
			return os.ErrInvalid
		}
	}

	// Check for existing entry
	var version int
	var deleted bool
	err = tx.QueryRowContext(ctx, `SELECT version, deleted FROM files WHERE path = ? ORDER BY version DESC LIMIT 1`, name).Scan(&version, &deleted)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if err == nil && !deleted {
		return os.ErrExist
	}

	// Make it
	now := time.Now().Unix()
	_, err = tx.ExecContext(ctx, `INSERT INTO files (created_at, version, path, mode, mod_time, is_dir) VALUES (?, ?, ?, ?, ?, 1)`, now, version+1, name, perm, now)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (fs *FS) RemoveAll(ctx context.Context, name string) error {
	name = path.Clean(name)
	tx, err := fs.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var isDir bool
	err = tx.QueryRowContext(ctx, `SELECT is_dir FROM files WHERE path = ? AND deleted = 0 ORDER BY version DESC LIMIT 1`, name).Scan(&isDir)
	if err == sql.ErrNoRows {
		return os.ErrNotExist
	}
	if err != nil {
		return err
	}

	now := time.Now().Unix()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted, blob_id)
		SELECT ?, version + 1, path, mode, mod_time, is_dir, 1, blob_id
		FROM files
		WHERE path = ? AND deleted = 0
		ORDER BY version DESC
		LIMIT 1`, now, name)
	if err != nil {
		return err
	}
	if isDir {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted, blob_id)
			SELECT ?, version + 1, path, mode, mod_time, is_dir, 1, blob_id
			FROM files
			WHERE path GLOB ? AND deleted = 0
			GROUP BY path
			HAVING version = MAX(version)`, now, name+"/*")
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (fs *FS) Rename(ctx context.Context, oldName, newName string) error {
	oldName = path.Clean(oldName)
	newName = path.Clean(newName)
	tx, err := fs.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().Unix()

	var isDir bool
	err = tx.QueryRowContext(ctx, `SELECT is_dir FROM files WHERE path = ? AND deleted = 0 ORDER BY version DESC LIMIT 1`, oldName).Scan(&isDir)
	if err == sql.ErrNoRows {
		return os.ErrNotExist
	}
	if err != nil {
		return err
	}

	// Create new entry at destination path
	_, err = tx.ExecContext(ctx, `
		INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, blob_id)
		SELECT ?, version + 1, ?, mode, mod_time, is_dir, blob_id
		FROM files
		WHERE path = ? AND deleted = 0
		ORDER BY version DESC
		LIMIT 1`, now, newName, oldName)
	if err != nil {
		return err
	}

	// Mark old path as deleted
	_, err = tx.ExecContext(ctx, `
		INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted, blob_id)
		SELECT ?, version + 1, path, mode, mod_time, is_dir, 1, blob_id
		FROM files
		WHERE path = ? AND deleted = 0
		ORDER BY version DESC
		LIMIT 1`, now, oldName)
	if err != nil {
		return err
	}

	// If it's a directory, rename all contents in bulk
	if isDir {
		oldPrefixLen := len(oldName) + 1 // +1 for the trailing slash

		// Create new entries at renamed paths (newName + suffix for each oldName + suffix)
		_, err = tx.ExecContext(ctx, `
			INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, blob_id)
			SELECT ?, version + 1, ? || SUBSTR(path, ?), mode, mod_time, is_dir, blob_id
			FROM files
			WHERE path GLOB ? AND deleted = 0
			GROUP BY path
			HAVING version = MAX(version)`, now, newName, oldPrefixLen, oldName+"/*")
		if err != nil {
			return err
		}

		// Mark old paths as deleted
		_, err = tx.ExecContext(ctx, `
			INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted, blob_id)
			SELECT ?, version + 1, path, mode, mod_time, is_dir, 1, blob_id
			FROM files
			WHERE path GLOB ? AND deleted = 0
			GROUP BY path
			HAVING version = MAX(version)`, now, oldName+"/*")
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (fs *FS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	name = path.Clean(name)
	if name == "/" {
		return &fileInfo{name: "/", isDir: true, mode: 0755}, nil
	}

	row := fs.db.QueryRowContext(ctx, ` SELECT COALESCE(b.size, 0), f.mode, f.mod_time, f.is_dir, f.deleted FROM files f LEFT JOIN blobs b ON f.blob_id = b.id WHERE f.path = ? ORDER BY f.version DESC LIMIT 1`, name)
	fi := &fileInfo{name: path.Base(name)}

	var deleted bool
	err := row.Scan(&fi.size, &fi.mode, &fi.modTime, &fi.isDir, &deleted)
	if err == sql.ErrNoRows || deleted {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, err
	}

	return fi, nil
}

func (fs *FS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	name = path.Clean(name)
	if name == "/" {
		return &file{db: fs.db, path: name, isDir: true}, nil
	}
	return openFile(ctx, fs.db, fs.blobsDir, name, flag, perm)
}

func deleteLocalBlobs(db *sql.DB, blobsDir string) (bool, error) {
	rows, err := db.Query(`SELECT id FROM blobs WHERE local_deleting = 1 AND local_deleted = 0 ORDER BY RANDOM() LIMIT 100`)
	if err != nil {
		return false, err
	}
	var blobIDs []string
	for rows.Next() {
		var blobID string
		if err := rows.Scan(&blobID); err != nil {
			rows.Close()
			return false, err
		}
		blobIDs = append(blobIDs, blobID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}

	for _, blobID := range blobIDs {
		blobPath := blobFilePath(blobsDir, blobID)
		deleted, err := tryDeleteBlob(blobPath)
		if err != nil {
			slog.Error("failed to delete local blob", "blob_id", blobID, "path", blobPath, "error", err)
			continue
		}
		if !deleted {
			continue
		}
		if _, err := db.Exec(`UPDATE blobs SET local_deleted = 1 WHERE id = ?`, blobID); err != nil {
			return false, err
		}
	}
	return len(blobIDs) > 0, nil
}

func tryDeleteBlob(path string) (bool, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		f.Close()
		slog.Warn("blob file is locked, skipping deletion", "path", path)
		return false, nil
	}
	err = os.Remove(path)
	f.Close()
	if err != nil {
		return false, err
	}
	return true, nil
}
