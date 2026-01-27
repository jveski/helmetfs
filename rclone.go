package main

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os/exec"
	"strings"
)

type Rclone struct {
	remotePath    string
	bwLimit       string
	downloadLimit chan struct{}
}

func NewRclone(remotePath, bwLimit string, historicalDownloadConcurrency int) *Rclone {
	return &Rclone{
		remotePath:    remotePath,
		bwLimit:       bwLimit,
		downloadLimit: make(chan struct{}, historicalDownloadConcurrency),
	}
}

func (r *Rclone) Sync(db *sql.DB, blobsDir string) (bool, error) {
	n1, err := r.batchOp(db, blobsDir,
		`SELECT id FROM blobs WHERE remote_written = 1 AND remote_deleting = 1 AND remote_deleted = 0 LIMIT 100`,
		`UPDATE blobs SET remote_deleted = 1 WHERE id = ?`,
		[]string{"delete", "--files-from-raw", "-", r.remotePath},
		"deleted blobs from remote")
	if err != nil {
		return false, err
	}
	n2, err := r.batchOp(db, blobsDir,
		`SELECT id FROM blobs WHERE local_written = 1 AND remote_written = 0 AND local_deleting = 0 LIMIT 100`,
		`UPDATE blobs SET remote_written = 1 WHERE id = ?`,
		[]string{"copy", "--no-update-dir-modtime", "--files-from-raw", "-", blobsDir, r.remotePath},
		"uploaded blobs to remote")
	if err != nil {
		return false, err
	}
	n3, err := r.batchOp(db, blobsDir,
		`SELECT id FROM blobs WHERE local_written = 0 AND remote_written = 1 AND remote_deleted = 0 LIMIT 100`,
		`UPDATE blobs SET local_written = 1 WHERE id = ?`,
		[]string{"copy", "--no-update-dir-modtime", "--files-from-raw", "-", r.remotePath, blobsDir},
		"downloaded blobs from remote")
	return n1+n2+n3 > 0, err
}

func (r *Rclone) batchOp(db *sql.DB, blobsDir, selectQuery, updateQuery string, rcloneArgs []string, logMsg string) (int, error) {
	rows, err := db.Query(selectQuery)
	if err != nil {
		return 0, err
	}
	var blobIDs []string
	for rows.Next() {
		var blobID string
		if err := rows.Scan(&blobID); err != nil {
			rows.Close()
			return 0, err
		}
		blobIDs = append(blobIDs, blobID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(blobIDs) == 0 {
		return 0, nil
	}

	var fileList strings.Builder
	for _, blobID := range blobIDs {
		// Use relative blob path (e.g., "ab/cd1234...") for --files-from-raw
		fileList.WriteString(blobID[:2])
		fileList.WriteByte('/')
		fileList.WriteString(blobID[2:])
		fileList.WriteByte('\n')
	}

	args := rcloneArgs
	if r.bwLimit != "" && r.bwLimit != "0" {
		args = append([]string{"--bwlimit", r.bwLimit}, args...)
	}
	cmd := exec.Command("rclone", args...)
	cmd.Stdin = strings.NewReader(fileList.String())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		slog.Error("rclone command failed", "args", args, "error", err, "stderr", stderr.String())
		return 0, err
	}

	for _, blobID := range blobIDs {
		if _, err := db.Exec(updateQuery, blobID); err != nil {
			return 0, err
		}
	}
	slog.Info(logMsg, "count", len(blobIDs))
	return len(blobIDs), nil
}

func (r *Rclone) CopyFile(src, dst string, toRemote bool) error {
	if toRemote {
		dst = r.remotePath + "/" + dst
	} else {
		src = r.remotePath + "/" + src
	}
	args := []string{"copyto", src, dst}
	if r.bwLimit != "" && r.bwLimit != "0" {
		args = append([]string{"--bwlimit", r.bwLimit}, args...)
	}
	cmd := exec.Command("rclone", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		slog.Error("rclone copyto failed", "src", src, "dst", dst, "error", err, "stderr", stderr.String())
		return err
	}
	return nil
}

func (r *Rclone) CatBlob(ctx context.Context, blobID string, w io.Writer) error {
	select {
	case r.downloadLimit <- struct{}{}:
		defer func() { <-r.downloadLimit }()
	case <-ctx.Done():
		return ctx.Err()
	}

	src := r.remotePath + "/" + blobID[:2] + "/" + blobID[2:]
	cmd := exec.Command("rclone", "cat", src)
	cmd.Stdout = w
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		slog.Error("rclone cat failed", "src", src, "error", err, "stderr", stderr.String())
		return err
	}
	return nil
}
