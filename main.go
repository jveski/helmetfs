package main

import (
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"flag"
	"html/template"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
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

//go:embed assets/status.html
var statusHTML string

//go:embed assets/browse.html
var browseHTML string

//go:embed assets/schema.sql
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

	var rc *Rclone
	if *rcloneRemote != "" {
		rc = NewRclone(*rcloneRemote, *rcloneBwLimit, *historicalDownloadConcurrency)
	}

	if err := restoreDatabase(dbPath, dbBackupPath, rc); err != nil {
		return err
	}

	db, err := sql.Open("sqlite3", "file:"+dbPath+"?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate")
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

	server := NewServer(db, blobsDir, rc)
	handler := logRequests(server)

	runLoop(10*time.Second, "garbage collection", func() (bool, error) {
		return takeOutTheTrash(db)
	})
	runLoop(10*time.Second, "local blob deletion", func() (bool, error) {
		return deleteLocalBlobs(db, blobsDir)
	})
	runLoop(time.Second, "integrity check", func() (bool, error) {
		return checkFileIntegrity(db, blobsDir, *integrityCheckInterval)
	})
	if rc != nil {
		runLoop(time.Second, "remote sync", func() (bool, error) {
			return rc.Sync(db, blobsDir)
		})
	}
	if *backupInterval > 0 && rc != nil {
		runLoop(*backupInterval, "database backup", func() (bool, error) {
			return false, backupDatabase(db, dbBackupPath, rc)
		})
	}

	return http.ListenAndServe(*listenAddr, handler)
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

func backupDatabase(db *sql.DB, backupPath string, rc *Rclone) error {
	tmpPath := backupPath + ".tmp"
	_, err := db.Exec(`VACUUM INTO ?`, tmpPath)
	if err != nil {
		return err
	}
	if err := os.Rename(tmpPath, backupPath); err != nil {
		return err
	}

	if err := rc.CopyFile(backupPath, "meta.db.backup", true); err != nil {
		return err
	}

	slog.Info("database backup completed")
	return nil
}

func restoreDatabase(dbPath, backupPath string, rc *Rclone) error {
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) || rc == nil {
		return nil // db still exists or no remote configured
	}

	if _, err := os.Stat(backupPath); err != nil {
		tmpPath := backupPath + ".tmp"
		slog.Info("local backup missing, attempting to download from remote")
		if err := rc.CopyFile("meta.db.backup", tmpPath, false); err != nil {
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
