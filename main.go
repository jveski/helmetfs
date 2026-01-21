package main

import (
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/net/webdav"
)

var (
	listenAddr             = flag.String("addr", ":8080", "address to listen on")
	ttl                    = flag.Duration("ttl", time.Hour*24, "TTL for retaining old file versions before compaction")
	uploadTTL              = flag.Duration("upload-ttl", 5*time.Minute, "TTL for pending blobs before marking for deletion")
	rcloneRemote           = flag.String("rclone-remote", "", "rclone remote path for blob storage (e.g. 'myremote:bucket/path')")
	rcloneBwLimit          = flag.String("rclone-bwlimit", "0", "rclone bandwidth limit (e.g. '20M' for 20 Mbit/s, '0' for unlimited)")
	integrityCheckInterval = flag.Duration("integrity-check-interval", 24*time.Hour, "minimum interval between integrity checks for each blob")
	backupInterval         = flag.Duration("backup-interval", 1*time.Hour, "interval between database backups (0 to disable)")
	debug                  = flag.Bool("debug", false, "enable debug logging")
)

const (
	dbPath       = "meta.db"
	dbBackupPath = "meta.db.backup"
)

//go:embed status.html
var statusHTML string

//go:embed schema.sql
var schema string

var statusTemplate = template.Must(template.New("status").Parse(statusHTML))

func main() {
	flag.Parse()
	if err := run(); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logLevel := slog.LevelInfo
	if *debug {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	if err := restoreDatabase(dbPath, dbBackupPath, *rcloneRemote, *rcloneBwLimit); err != nil {
		return err
	}

	db, err := sql.Open("sqlite3", "file:"+dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	_, err = db.Exec(schema)
	if err != nil {
		return err
	}

	blobsDir := "blobs"
	if err := os.MkdirAll(blobsDir, 0755); err != nil {
		return err
	}
	if err := initBlobDirs(blobsDir); err != nil {
		return err
	}

	mux := newRouter(db, blobsDir)
	handler := logRequests(mux)

	runLoop(10*time.Second, "garbage collection", func() (bool, error) {
		return takeOutTheTrash(db)
	})
	runLoop(10*time.Second, "local blob deletion", func() (bool, error) {
		return deleteLocalBlobs(db, blobsDir)
	})
	runLoop(time.Second, "integrity check", func() (bool, error) {
		return checkFileIntegrity(db, blobsDir, *integrityCheckInterval)
	})
	if *rcloneRemote != "" {
		runLoop(time.Second, "remote sync", func() (bool, error) {
			return syncRemote(db, blobsDir, *rcloneRemote, *rcloneBwLimit)
		})
	}
	if *backupInterval > 0 && *rcloneRemote != "" {
		runLoop(*backupInterval, "database backup", func() (bool, error) {
			return false, backupDatabase(db, dbBackupPath, *rcloneRemote, *rcloneBwLimit)
		})
	}

	return http.ListenAndServe(*listenAddr, handler)
}

func newRouter(db *sql.DB, blobsDir string) http.Handler {
	dav := &webdav.Handler{
		FileSystem: &FS{db: db, blobsDir: blobsDir},
		LockSystem: webdav.NewMemLS(),
		Logger: func(r *http.Request, err error) {
			if err != nil {
				slog.Error("webdav error", "error", err)
			}
		},
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && r.Method == "GET" {
			handleStatus(w, r, db)
			return
		}
		if r.URL.Path == "/api/restore" && r.Method == "POST" {
			handleRestore(w, r, db)
			return
		}
		dav.ServeHTTP(w, r)
	})
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		slog.Debug("request started", "method", r.Method, "url", r.URL.String())
		next.ServeHTTP(w, r)
		slog.Debug("request completed", "method", r.Method, "url", r.URL.String(), "duration", time.Since(start))
	})
}

func handleRestore(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	timestamp := r.FormValue("timestamp")
	if timestamp == "" {
		http.Error(w, "timestamp is required", http.StatusBadRequest)
		return
	}
	ts, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		http.Error(w, "invalid timestamp format, expected RFC3339", http.StatusBadRequest)
		return
	}
	if ts.After(time.Now()) {
		http.Error(w, "cannot restore to a future time", http.StatusBadRequest)
		return
	}
	if ts.Before(time.Now().Add(-*ttl)) {
		http.Error(w, "cannot restore beyond the retention period", http.StatusBadRequest)
		return
	}

	tsUnix := ts.Unix()
	now := time.Now().Unix()

	_, err = db.ExecContext(r.Context(), `
		INSERT INTO files (created_at, version, path, mode, mod_time, is_dir, deleted, blob_id)
		SELECT
			?,
			curr.version + 1,
			curr.path,
			COALESCE(hist.mode, curr.mode),
			COALESCE(hist.mod_time, curr.mod_time),
			COALESCE(hist.is_dir, curr.is_dir),
			CASE WHEN hist.id IS NULL THEN 1 ELSE hist.deleted END,
			hist.blob_id
		FROM (
			SELECT path, MAX(version) as version FROM files GROUP BY path
		) latest
		JOIN files curr ON curr.path = latest.path AND curr.version = latest.version
		LEFT JOIN files hist ON hist.path = curr.path
			AND hist.created_at <= ?
			AND hist.version = (
				SELECT MAX(version) FROM files
				WHERE path = curr.path AND created_at <= ?
			)
		WHERE
			(hist.id IS NULL AND curr.deleted = 0)
			OR (hist.id IS NOT NULL AND hist.deleted != curr.deleted)
			OR (hist.id IS NOT NULL AND hist.blob_id IS NOT curr.blob_id)
	`, now, tsUnix, tsUnix)
	if err != nil {
		http.Error(w, "restore failed", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func handleStatus(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	var pendingDownloads, pendingUploads, pendingDeletes int64
	db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM blobs WHERE local_written = 0 AND remote_written = 1 AND remote_deleted = 0`).Scan(&pendingDownloads)
	db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM blobs WHERE local_written = 1 AND remote_written = 0 AND local_deleting = 0`).Scan(&pendingUploads)
	db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM blobs WHERE remote_written = 1 AND remote_deleting = 1 AND remote_deleted = 0`).Scan(&pendingDeletes)

	var overdueIntegrity int64
	cutoff := time.Now().Add(-*integrityCheckInterval).Unix()
	db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM blobs WHERE local_written = 1 AND remote_written = 1 AND local_deleting = 0 AND checksum IS NOT NULL AND (last_integrity_check IS NULL OR last_integrity_check < ?)`, cutoff).Scan(&overdueIntegrity)

	var activeFiles int64
	db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM files f1 WHERE f1.version = (SELECT MAX(f2.version) FROM files f2 WHERE f2.path = f1.path) AND f1.deleted = 0`).Scan(&activeFiles)

	var localSpace, remoteSpace int64
	db.QueryRowContext(r.Context(), `SELECT COALESCE(SUM(size), 0) FROM blobs WHERE local_written = 1 AND local_deleted = 0`).Scan(&localSpace)
	db.QueryRowContext(r.Context(), `SELECT COALESCE(SUM(size), 0) FROM blobs WHERE remote_written = 1 AND remote_deleted = 0`).Scan(&remoteSpace)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	statusTemplate.Execute(w, map[string]any{
		"PendingDownloads": pendingDownloads,
		"PendingUploads":   pendingUploads,
		"PendingDeletes":   pendingDeletes,
		"OverdueIntegrity": overdueIntegrity,
		"ActiveFiles":      activeFiles,
		"LocalSpace":       formatBytes(localSpace),
		"RemoteSpace":      formatBytes(remoteSpace),
	})
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func takeOutTheTrash(db *sql.DB) (bool, error) {
	blobCutoff := time.Now().Add(-*uploadTTL).Unix()
	result, err := db.Exec(`UPDATE blobs SET local_deleting = 1 WHERE local_written = 0 AND local_deleting = 0 AND creation_time < ?`, blobCutoff)
	if err != nil {
		return false, err
	}
	if n, _ := result.RowsAffected(); n > 0 {
		slog.Info("marked orphaned blobs for deletion", "count", n)
	}

	fileCutoff := time.Now().Add(-*ttl).Unix()
	result, err = db.Exec(`
		DELETE FROM files WHERE id IN (
			SELECT f.id FROM files f
			WHERE f.created_at < ?
			AND (
				f.deleted = 1
				OR EXISTS (SELECT 1 FROM files f2 WHERE f2.path = f.path AND f2.version > f.version)
			)
		)`, fileCutoff)
	if err != nil {
		return false, err
	}
	if n, _ := result.RowsAffected(); n > 0 {
		slog.Info("compacted old file versions", "count", n)
	}
	return false, nil
}

func checkFileIntegrity(db *sql.DB, blobsDir string, checkInterval time.Duration) (bool, error) {
	cutoff := time.Now().Add(-checkInterval).Unix()
	var blobID string
	var checksum string
	err := db.QueryRow(`
		SELECT id, checksum FROM blobs
		WHERE local_written = 1
		AND remote_written = 1
		AND local_deleting = 0
		AND checksum IS NOT NULL
		AND (last_integrity_check IS NULL OR last_integrity_check < ?)
		ORDER BY last_integrity_check ASC NULLS FIRST
		LIMIT 1`, cutoff).Scan(&blobID, &checksum)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	now := time.Now().Unix()
	blobPath := blobFilePath(blobsDir, blobID)
	f, err := os.Open(blobPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return false, err
		}
		slog.Warn("blob file missing during integrity check", "blob_id", blobID)
		_, err = db.Exec(`UPDATE blobs SET local_written = 0, last_integrity_check = NULL WHERE id = ? AND remote_written = 1`, blobID)
		return err == nil, err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, err
	}
	actualChecksum := hex.EncodeToString(h.Sum(nil))
	if actualChecksum != checksum {
		slog.Error("integrity check failed", "blob_id", blobID, "expected", checksum, "actual", actualChecksum)
		_, err = db.Exec(`UPDATE blobs SET local_written = 0, last_integrity_check = NULL WHERE id = ? AND remote_written = 1`, blobID)
		return err == nil, err
	}

	_, err = db.Exec(`UPDATE blobs SET last_integrity_check = ? WHERE id = ?`, now, blobID)
	return err == nil, err
}

func backupDatabase(db *sql.DB, backupPath, remotePath, bwLimit string) error {
	tmpPath := backupPath + ".tmp"
	_, err := db.Exec(`VACUUM INTO ?`, tmpPath)
	if err != nil {
		return err
	}
	if err := os.Rename(tmpPath, backupPath); err != nil {
		return err
	}

	err = rcloneCopyFile(backupPath, remotePath+"/meta.db.backup", bwLimit)
	if err != nil {
		return err
	}

	slog.Info("database backup completed")
	return nil
}

func restoreDatabase(dbPath, backupPath, remotePath, bwLimit string) error {
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) || remotePath == "" {
		return nil // db still exists
	}

	if _, err := os.Stat(backupPath); err != nil {
		tmpPath := backupPath + ".tmp"
		slog.Info("local backup missing, attempting to download from remote")
		if err := rcloneCopyFile(remotePath+"/meta.db.backup", tmpPath, bwLimit); err != nil {
			return err
		}

		if _, err := os.Stat(tmpPath); err != nil {
			slog.Info("database backup is missing - initializing new db")
			return nil
		}

		slog.Info("database backup downloaded from remote")
		return os.Rename(tmpPath, backupPath)
	}
	return nil
}

func runLoop(interval time.Duration, name string, fn func() (bool, error)) {
	go func() {
		for {
			more, err := fn()
			if err != nil {
				slog.Error(name+" failed", "error", err)
			}
			if !more {
				jitter := time.Duration(rand.Int64N(int64(2 * time.Second)))
				time.Sleep(interval + jitter)
			}
		}
	}()
}
