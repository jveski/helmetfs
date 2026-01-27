package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/net/webdav"
)

var (
	listenAddr                    = flag.String("addr", ":8080", "address to listen on")
	ttl                           = flag.Duration("ttl", time.Hour*24, "TTL for retaining old file versions before compaction")
	uploadTTL                     = flag.Duration("upload-ttl", 5*time.Minute, "TTL for pending blobs before marking for deletion")
	rcloneRemote                  = flag.String("rclone-remote", "", "rclone remote path for blob storage (e.g. 'myremote:bucket/path')")
	rcloneBwLimit                 = flag.String("rclone-bwlimit", "0", "rclone bandwidth limit (e.g. '20M' for 20 Mbit/s, '0' for unlimited)")
	integrityCheckInterval        = flag.Duration("integrity-check-interval", 24*time.Hour, "minimum interval between integrity checks for each blob")
	backupInterval                = flag.Duration("backup-interval", 1*time.Hour, "interval between database backups (0 to disable)")
	historicalDownloadConcurrency = flag.Int("historical-download-concurrency", 4, "max concurrent historical file downloads from remote storage")
	debug                         = flag.Bool("debug", false, "enable debug logging")
)

const (
	dbPath       = "meta.db"
	dbBackupPath = "meta.db.backup"
)

var historicalDownloadSem chan struct{}

//go:embed status.html
var statusHTML string

//go:embed browse.html
var browseHTML string

//go:embed schema.sql
var schema string

var (
	statusTemplate = template.Must(template.New("status").Parse(statusHTML))
	browseTemplate = template.Must(template.New("browse").Parse(browseHTML))
)

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

	historicalDownloadSem = make(chan struct{}, *historicalDownloadConcurrency)

	if err := restoreDatabase(dbPath, dbBackupPath, *rcloneRemote, *rcloneBwLimit); err != nil {
		return err
	}

	db, err := sql.Open("sqlite3", "file:"+dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return err
	}
	defer db.Close()

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
				if os.IsExist(err) || os.IsNotExist(err) {
					slog.Debug("webdav error", "error", err)
				} else {
					slog.Error("webdav error", "error", err)
				}
			}
		},
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && r.Method == "GET" {
			handleBrowse(w, r, db)
			return
		}
		if r.URL.Path == "/status" && r.Method == "GET" {
			handleStatus(w, r, db)
			return
		}
		if r.URL.Path == "/api/restore-file" && r.Method == "POST" {
			handleRestoreFile(w, r, db)
			return
		}
		if r.URL.Path == "/api/download-historical" && r.Method == "GET" {
			handleDownloadHistorical(w, r, db, blobsDir)
			return
		}
		if r.URL.Path == "/api/activity-timeline" && r.Method == "GET" {
			handleActivityTimeline(w, r, db)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/browse") && r.Method == "GET" {
			handleBrowse(w, r, db)
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

func handleRestoreFile(w http.ResponseWriter, r *http.Request, db *sql.DB) {
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
		http.Error(w, "cannot restore from a future time", http.StatusBadRequest)
		return
	}
	if ts.Before(time.Now().Add(-*ttl)) {
		http.Error(w, "cannot restore beyond the retention period", http.StatusBadRequest)
		return
	}

	filePath := strings.TrimSpace(r.FormValue("path"))
	if filePath == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(filePath, "/") {
		filePath = "/" + filePath
	}
	filePath = path.Clean(filePath)

	tsUnix := ts.Unix()
	now := time.Now().Unix()

	// Restore single file/directory (or directory tree)
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
			SELECT path, MAX(version) as version FROM files
			WHERE path = ? OR path LIKE ? || '/%'
			GROUP BY path
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
	`, now, filePath, filePath, tsUnix, tsUnix)
	if err != nil {
		http.Error(w, "restore failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

func handleDownloadHistorical(w http.ResponseWriter, r *http.Request, db *sql.DB, blobsDir string) {
	atParam := r.URL.Query().Get("at")
	filePath := r.URL.Query().Get("path")

	if atParam == "" || filePath == "" {
		http.Error(w, "at and path parameters required", http.StatusBadRequest)
		return
	}

	ts, err := time.Parse(time.RFC3339, atParam)
	if err != nil {
		http.Error(w, "invalid timestamp format, expected RFC3339", http.StatusBadRequest)
		return
	}
	if ts.After(time.Now()) {
		http.Error(w, "cannot download from future time", http.StatusBadRequest)
		return
	}
	if ts.Before(time.Now().Add(-*ttl)) {
		http.Error(w, "cannot download beyond retention period", http.StatusBadRequest)
		return
	}

	tsUnix := ts.Unix()

	// Look up blob_id at historical timestamp
	var blobID string
	var localWritten, remoteWritten bool
	var size int64
	err = db.QueryRowContext(r.Context(), `
		SELECT f.blob_id, b.local_written, b.remote_written, b.size
		FROM files f
		JOIN blobs b ON f.blob_id = b.id
		WHERE f.path = ? AND f.is_dir = 0 AND f.deleted = 0
		AND f.created_at <= ?
		AND f.version = (SELECT MAX(version) FROM files WHERE path = ? AND created_at <= ?)
	`, filePath, tsUnix, filePath, tsUnix).Scan(&blobID, &localWritten, &remoteWritten, &size)

	if err == sql.ErrNoRows {
		http.Error(w, "file not found at timestamp", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	blobPath := blobFilePath(blobsDir, blobID)

	// If blob is local, serve from disk
	if localWritten {
		w.Header().Set("Content-Disposition", "attachment; filename=\""+path.Base(filePath)+"\"")
		http.ServeFile(w, r, blobPath)
		return
	}

	// If blob is only on remote, stream directly without downloading locally
	if remoteWritten && *rcloneRemote != "" {
		remoteSrc := *rcloneRemote + "/" + blobID[:2] + "/" + blobID[2:]
		w.Header().Set("Content-Disposition", "attachment; filename=\""+path.Base(filePath)+"\"")
		w.Header().Set("Content-Type", "application/octet-stream")
		if size > 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		}

		// Acquire semaphore - throttle concurrent historical downloads
		// Headers sent first so client knows download is starting
		select {
		case historicalDownloadSem <- struct{}{}:
			defer func() { <-historicalDownloadSem }()
		case <-r.Context().Done():
			return
		}

		// Stream without bandwidth limit - historical downloads should be fast
		if err := rcloneCatFile(remoteSrc, w); err != nil {
			// Headers already sent, can't send error response
			slog.Error("failed to stream historical file from remote", "path", filePath, "blob_id", blobID, "error", err)
		}
		return
	}

	http.Error(w, "file content not available", http.StatusNotFound)
}

func handleActivityTimeline(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	dirPath := r.URL.Query().Get("path")
	if dirPath == "" {
		dirPath = "/"
	}
	dirPath = path.Clean(dirPath)

	cutoff := time.Now().Add(-*ttl).Unix()
	now := time.Now().Unix()

	prefix := dirPath
	if prefix != "/" {
		prefix += "/"
	}

	// Query distinct timestamps of changes within this directory
	var rows *sql.Rows
	var err error
	if dirPath == "/" {
		// Root: show all activity
		rows, err = db.QueryContext(r.Context(), `
			SELECT DISTINCT created_at
			FROM files
			WHERE created_at >= ? AND created_at <= ?
			ORDER BY created_at ASC
		`, cutoff, now)
	} else {
		// Specific directory: show activity for files in this directory (direct children only)
		rows, err = db.QueryContext(r.Context(), `
			SELECT DISTINCT created_at
			FROM files
			WHERE created_at >= ? AND created_at <= ?
			AND (path = ? OR path GLOB ?)
			AND path NOT GLOB ?
			ORDER BY created_at ASC
		`, cutoff, now, dirPath, prefix+"*", prefix+"*/*")
	}
	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var timestamps []int64
	for rows.Next() {
		var ts int64
		if err := rows.Scan(&ts); err != nil {
			continue
		}
		timestamps = append(timestamps, ts)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"timestamps": timestamps,
		"ttl_start":  cutoff,
		"ttl_end":    now,
	})
}

func handleBrowse(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	// Extract and clean path - support both / and /browse routes
	dirPath := r.URL.Path
	if strings.HasPrefix(dirPath, "/browse") {
		dirPath = strings.TrimPrefix(dirPath, "/browse")
	}
	if dirPath == "" {
		dirPath = "/"
	}
	dirPath = path.Clean(dirPath)

	// Parse optional timestamp for historical browsing
	var atTime *time.Time
	var atUnix int64
	var timestamp string
	if atParam := r.URL.Query().Get("at"); atParam != "" {
		ts, err := time.Parse(time.RFC3339, atParam)
		if err != nil {
			http.Error(w, "invalid timestamp format, expected RFC3339", http.StatusBadRequest)
			return
		}
		if ts.After(time.Now()) {
			http.Error(w, "cannot browse future time", http.StatusBadRequest)
			return
		}
		if ts.Before(time.Now().Add(-*ttl)) {
			http.Error(w, "cannot browse beyond retention period", http.StatusBadRequest)
			return
		}
		atTime = &ts
		atUnix = ts.Unix()
		timestamp = atParam
	}

	// Build breadcrumbs (include timestamp param in links for historical mode)
	type breadcrumb struct {
		Name string
		Path string
	}
	atSuffix := ""
	if atTime != nil {
		atSuffix = "?at=" + url.QueryEscape(timestamp)
	}
	breadcrumbs := []breadcrumb{{Name: "Home", Path: "/" + atSuffix}}
	if dirPath != "/" {
		parts := strings.Split(dirPath, "/")
		currentPath := ""
		for _, part := range parts {
			if part == "" {
				continue
			}
			currentPath += "/" + part
			breadcrumbs = append(breadcrumbs, breadcrumb{Name: part, Path: "/browse" + currentPath + atSuffix})
		}
	}

	// Query directory contents
	prefix := dirPath
	if prefix != "/" {
		prefix += "/"
	}

	type fileEntry struct {
		Name        string
		Link        string
		FilePath    string
		Size        string
		ModTime     string
		IsDir       bool
		BackedUp    bool
		LocalCached bool
	}

	rows, err := queryDirectoryEntries(r.Context(), db, prefix, atUnix)
	if err != nil {
		http.Error(w, "failed to read directory", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var files []fileEntry
	for rows.Next() {
		var filePath string
		var size, modTime int64
		var isDir, remoteWritten, deleted, localWritten bool
		if err := rows.Scan(&filePath, &size, &modTime, &isDir, &remoteWritten, &deleted, &localWritten); err != nil {
			http.Error(w, "failed to read file info", http.StatusInternalServerError)
			return
		}
		if deleted {
			continue
		}
		entry := fileEntry{
			Name:        path.Base(filePath),
			FilePath:    filePath,
			ModTime:     time.Unix(modTime, 0).Format("Jan 2, 2006 3:04 PM"),
			IsDir:       isDir,
			BackedUp:    remoteWritten,
			LocalCached: localWritten,
		}
		if isDir {
			entry.Link = "/browse" + filePath + atSuffix
		} else if atTime != nil {
			entry.Link = "/api/download-historical?path=" + url.QueryEscape(filePath) + "&at=" + url.QueryEscape(timestamp)
			entry.Size = formatBytes(size)
		} else {
			entry.Link = filePath
			entry.Size = formatBytes(size)
		}
		files = append(files, entry)
	}

	// Prepare template data
	templateData := map[string]any{
		"Path":           dirPath,
		"Breadcrumbs":    breadcrumbs,
		"Files":          files,
		"HistoricalMode": atTime != nil,
		"Timestamp":      timestamp,
		"TTLSeconds":     int64(ttl.Seconds()),
		"TTLHuman":       formatDuration(*ttl),
	}
	if atTime != nil {
		templateData["FormattedTimestamp"] = atTime.Format("Jan 2, 2006 3:04 PM")
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	browseTemplate.Execute(w, templateData)
}

func queryDirectoryEntries(ctx context.Context, db *sql.DB, prefix string, atUnix int64) (*sql.Rows, error) {
	if atUnix > 0 {
		// Historical query - shows files as they existed at the timestamp
		return db.QueryContext(ctx, `
			WITH latest AS (
				SELECT *, ROW_NUMBER() OVER (PARTITION BY path ORDER BY version DESC) as rn
				FROM files
				WHERE path GLOB ? AND path NOT GLOB ? AND created_at <= ?
			)
			SELECT f.path, COALESCE(b.size, 0), f.mod_time, f.is_dir,
			       COALESCE(b.remote_written, 0), f.deleted, COALESCE(b.local_written, 0)
			FROM latest f
			LEFT JOIN blobs b ON f.blob_id = b.id
			WHERE f.rn = 1
			ORDER BY f.is_dir DESC, f.path`, prefix+"*", prefix+"*/*", atUnix)
	}
	// Current query - shows latest version of non-deleted files
	// Returns constant 0 for deleted and 1 for local_written to match historical query columns
	return db.QueryContext(ctx, `
		WITH latest AS (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY path ORDER BY version DESC) as rn
			FROM files
			WHERE path GLOB ? AND path NOT GLOB ?
		)
		SELECT f.path, COALESCE(b.size, 0), f.mod_time, f.is_dir,
		       COALESCE(b.remote_written, 0), 0, 1
		FROM latest f
		LEFT JOIN blobs b ON f.blob_id = b.id
		WHERE f.rn = 1 AND f.deleted = 0
		ORDER BY f.is_dir DESC, f.path`, prefix+"*", prefix+"*/*")
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
	db.QueryRowContext(r.Context(), `
		WITH latest AS (
			SELECT deleted, ROW_NUMBER() OVER (PARTITION BY path ORDER BY version DESC) as rn
			FROM files
		)
		SELECT COUNT(*) FROM latest WHERE rn = 1 AND deleted = 0`).Scan(&activeFiles)

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

func formatDuration(d time.Duration) string {
	hours := int(d.Hours())
	if hours >= 24 && hours%24 == 0 {
		days := hours / 24
		if days == 1 {
			return "1 day"
		}
		return fmt.Sprintf("%d days", days)
	}
	if hours == 1 {
		return "1 hour"
	}
	return fmt.Sprintf("%d hours", hours)
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
