package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/webdav"
)

// Server handles HTTP requests for the HelmetFS WebDAV server.
type Server struct {
	db       *sql.DB
	blobsDir string
	rclone   *Rclone
	dav      *webdav.Handler
}

// NewServer creates a new Server with the given database and blob directory.
func NewServer(db *sql.DB, blobsDir string, rc *Rclone) *Server {
	s := &Server{
		db:       db,
		blobsDir: blobsDir,
		rclone:   rc,
	}
	s.dav = &webdav.Handler{
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
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" && r.Method == "GET" {
		s.handleBrowse(w, r)
		return
	}
	if r.URL.Path == "/status" && r.Method == "GET" {
		s.handleStatus(w, r)
		return
	}
	if r.URL.Path == "/api/restore-file" && r.Method == "POST" {
		s.handleRestoreFile(w, r)
		return
	}
	if r.URL.Path == "/api/download-historical" && r.Method == "GET" {
		s.handleDownloadHistorical(w, r)
		return
	}
	if r.URL.Path == "/api/activity-timeline" && r.Method == "GET" {
		s.handleActivityTimeline(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/browse") && r.Method == "GET" {
		s.handleBrowse(w, r)
		return
	}
	s.dav.ServeHTTP(w, r)
}

func (s *Server) handleRestoreFile(w http.ResponseWriter, r *http.Request) {
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
	_, err = s.db.ExecContext(r.Context(), `
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

func (s *Server) handleDownloadHistorical(w http.ResponseWriter, r *http.Request) {
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
	err = s.db.QueryRowContext(r.Context(), `
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

	blobPath := blobFilePath(s.blobsDir, blobID)

	// If blob is local, serve from disk
	if localWritten {
		w.Header().Set("Content-Disposition", "attachment; filename=\""+path.Base(filePath)+"\"")
		http.ServeFile(w, r, blobPath)
		return
	}

	// If blob is only on remote, stream directly without downloading locally
	if remoteWritten && s.rclone != nil {
		w.Header().Set("Content-Disposition", "attachment; filename=\""+path.Base(filePath)+"\"")
		w.Header().Set("Content-Type", "application/octet-stream")
		if size > 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		}

		// Stream without bandwidth limit - historical downloads should be fast
		// CatBlob handles semaphore acquisition internally
		if err := s.rclone.CatBlob(r.Context(), blobID, w); err != nil {
			// Headers already sent, can't send error response
			slog.Error("failed to stream historical file from remote", "path", filePath, "blob_id", blobID, "error", err)
		}
		return
	}

	http.Error(w, "file content not available", http.StatusNotFound)
}

func (s *Server) handleActivityTimeline(w http.ResponseWriter, r *http.Request) {
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
		rows, err = s.db.QueryContext(r.Context(), `
			SELECT DISTINCT created_at
			FROM files
			WHERE created_at >= ? AND created_at <= ?
			ORDER BY created_at ASC
		`, cutoff, now)
	} else {
		// Specific directory: show activity for files in this directory (direct children only)
		rows, err = s.db.QueryContext(r.Context(), `
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

func (s *Server) handleBrowse(w http.ResponseWriter, r *http.Request) {
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

	rows, err := s.queryDirectoryEntries(r.Context(), prefix, atUnix)
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

func (s *Server) queryDirectoryEntries(ctx context.Context, prefix string, atUnix int64) (*sql.Rows, error) {
	// When atUnix > 0 (historical): show files as they existed at that timestamp, including deleted
	// When atUnix = 0 (current): show latest version of non-deleted files only
	return s.db.QueryContext(ctx, `
		WITH latest AS (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY path ORDER BY version DESC) as rn
			FROM files
			WHERE path GLOB ?1 AND path NOT GLOB ?2
			  AND (?3 = 0 OR created_at <= ?3)
		)
		SELECT f.path, COALESCE(b.size, 0), f.mod_time, f.is_dir,
		       COALESCE(b.remote_written, 0),
		       CASE WHEN ?3 > 0 THEN f.deleted ELSE 0 END,
		       CASE WHEN ?3 > 0 THEN COALESCE(b.local_written, 0) ELSE 1 END
		FROM latest f
		LEFT JOIN blobs b ON f.blob_id = b.id
		WHERE f.rn = 1 AND (?3 > 0 OR f.deleted = 0)
		ORDER BY f.is_dir DESC, f.path`, prefix+"*", prefix+"*/*", atUnix)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	var pendingDownloads, pendingUploads, pendingDeletes int64
	s.db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM blobs WHERE local_written = 0 AND remote_written = 1 AND remote_deleted = 0`).Scan(&pendingDownloads)
	s.db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM blobs WHERE local_written = 1 AND remote_written = 0 AND local_deleting = 0`).Scan(&pendingUploads)
	s.db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM blobs WHERE remote_written = 1 AND remote_deleting = 1 AND remote_deleted = 0`).Scan(&pendingDeletes)

	var overdueIntegrity int64
	cutoff := time.Now().Add(-*integrityCheckInterval).Unix()
	s.db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM blobs WHERE local_written = 1 AND remote_written = 1 AND local_deleting = 0 AND checksum IS NOT NULL AND (last_integrity_check IS NULL OR last_integrity_check < ?)`, cutoff).Scan(&overdueIntegrity)

	var activeFiles int64
	s.db.QueryRowContext(r.Context(), `
		WITH latest AS (
			SELECT deleted, ROW_NUMBER() OVER (PARTITION BY path ORDER BY version DESC) as rn
			FROM files
		)
		SELECT COUNT(*) FROM latest WHERE rn = 1 AND deleted = 0`).Scan(&activeFiles)

	var localSpace, remoteSpace int64
	s.db.QueryRowContext(r.Context(), `SELECT COALESCE(SUM(size), 0) FROM blobs WHERE local_written = 1 AND local_deleted = 0`).Scan(&localSpace)
	s.db.QueryRowContext(r.Context(), `SELECT COALESCE(SUM(size), 0) FROM blobs WHERE remote_written = 1 AND remote_deleted = 0`).Scan(&remoteSpace)

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

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		slog.Debug("request started", "method", r.Method, "url", r.URL.String())
		next.ServeHTTP(w, r)
		slog.Debug("request completed", "method", r.Method, "url", r.URL.String(), "duration", time.Since(start))
	})
}
