package main

import (
	"context"
	"database/sql"
	"os"
	"path"
	"time"

	"golang.org/x/net/webdav"
)

type FS struct {
	db       *sql.DB
	blobsDir string
}

func (fs *FS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	name = path.Clean(name)
	parent := path.Dir(name)
	now := time.Now().Unix()

	res, err := fs.db.ExecContext(ctx, `
		INSERT INTO files (created_at, version, path, mode, mod_time, is_dir)
		SELECT ?, COALESCE((SELECT version FROM files WHERE path = ? ORDER BY version DESC LIMIT 1), 0) + 1, ?, ?, ?, 1
		WHERE (? IN ('/', '.') OR (SELECT is_dir AND NOT deleted FROM files WHERE path = ? ORDER BY version DESC LIMIT 1))
		  AND NOT COALESCE((SELECT NOT deleted FROM files WHERE path = ? ORDER BY version DESC LIMIT 1), 0)`,
		now, name, name, perm, now, parent, parent, name)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 0 {
		return nil
	}

	// Determine which error: parent missing vs not-a-dir vs already exists
	if parent != "/" && parent != "." {
		var parentIsDir, parentDeleted bool
		err := fs.db.QueryRowContext(ctx, `SELECT is_dir, deleted FROM files WHERE path = ? ORDER BY version DESC LIMIT 1`, parent).Scan(&parentIsDir, &parentDeleted)
		if err == sql.ErrNoRows || parentDeleted {
			return os.ErrNotExist
		}
		if !parentIsDir {
			return os.ErrInvalid
		}
	}
	return os.ErrExist
}

func (fs *FS) RemoveAll(ctx context.Context, name string) error {
	name = path.Clean(name)
	now := time.Now().Unix()
	res, err := fs.db.ExecContext(ctx, `
		INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted, blob_id)
		SELECT ?, version + 1, path, mode, mod_time, is_dir, 1, blob_id
		FROM files
		WHERE deleted = 0
		  AND (path = ? OR (path GLOB ? AND (SELECT is_dir FROM files WHERE path = ? AND deleted = 0 ORDER BY version DESC LIMIT 1)))
		GROUP BY path
		HAVING version = MAX(version)`, now, name, name+"/*", name)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return os.ErrNotExist
	}
	return nil
}

func (fs *FS) Rename(ctx context.Context, oldName, newName string) error {
	oldName = path.Clean(oldName)
	newName = path.Clean(newName)
	oldPrefixLen := len(oldName) + 1 // +1 for the trailing slash
	now := time.Now().Unix()

	tx, err := fs.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Create new entries at destination paths (handles both the item and children if directory)
	res, err := tx.ExecContext(ctx, `
		INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, blob_id)
		SELECT ?, version + 1, ? || SUBSTR(path, ?), mode, mod_time, is_dir, blob_id
		FROM files
		WHERE deleted = 0
		  AND (path = ? OR (path GLOB ? AND (SELECT is_dir FROM files WHERE path = ? AND deleted = 0 ORDER BY version DESC LIMIT 1)))
		GROUP BY path
		HAVING version = MAX(version)`, now, newName, oldPrefixLen, oldName, oldName+"/*", oldName)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return os.ErrNotExist
	}

	// Mark old paths as deleted
	_, err = tx.ExecContext(ctx, `
		INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted, blob_id)
		SELECT ?, version + 1, path, mode, mod_time, is_dir, 1, blob_id
		FROM files
		WHERE deleted = 0
		  AND (path = ? OR (path GLOB ? AND (SELECT is_dir FROM files WHERE path = ? AND deleted = 0 ORDER BY version DESC LIMIT 1)))
		GROUP BY path
		HAVING version = MAX(version)`, now, oldName, oldName+"/*", oldName)
	if err != nil {
		return err
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
