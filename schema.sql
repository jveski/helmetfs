PRAGMA auto_vacuum=FULL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS blobs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	creation_time INTEGER NOT NULL,
	modified_time INTEGER NOT NULL,
	last_integrity_check INTEGER,

	local_written INTEGER NOT NULL DEFAULT 0,
	local_deleting INTEGER NOT NULL DEFAULT 0,
	local_deleted INTEGER NOT NULL DEFAULT 0,
	remote_written INTEGER NOT NULL DEFAULT 0,
	remote_deleting INTEGER NOT NULL DEFAULT 0,
	remote_deleted INTEGER NOT NULL DEFAULT 0,

	size INTEGER,
	checksum TEXT UNIQUE
) STRICT;

CREATE INDEX IF NOT EXISTS idx_blobs_orphan_cleanup ON blobs (creation_time)
	WHERE local_written = 0 AND local_deleting = 0;

CREATE INDEX IF NOT EXISTS idx_blobs_pending_download ON blobs (id) WHERE local_written = 0 AND remote_written = 1 AND remote_deleted = 0;
CREATE INDEX IF NOT EXISTS idx_blobs_pending_upload ON blobs (id) WHERE local_written = 1 AND remote_written = 0 AND local_deleting = 0;
CREATE INDEX IF NOT EXISTS idx_blobs_pending_remote_delete ON blobs (id) WHERE remote_written = 1 AND remote_deleting = 1 AND remote_deleted = 0;
CREATE INDEX IF NOT EXISTS idx_blobs_local_deleting ON blobs (id) WHERE local_deleting = 1 AND local_deleted = 0;
CREATE INDEX IF NOT EXISTS idx_blobs_integrity_check ON blobs (last_integrity_check) WHERE local_written = 1 AND remote_written = 1 AND local_deleting = 0 AND checksum IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_blobs_local_storage ON blobs (size) WHERE local_written = 1 AND local_deleted = 0;
CREATE INDEX IF NOT EXISTS idx_blobs_remote_storage ON blobs (size) WHERE remote_written = 1 AND remote_deleted = 0;

CREATE TABLE IF NOT EXISTS files (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	created_at INTEGER NOT NULL,
	version INTEGER NOT NULL DEFAULT 1,
	path TEXT NOT NULL,
	mode INTEGER NOT NULL,
	mod_time INTEGER NOT NULL,
	is_dir INTEGER NOT NULL,
	deleted INTEGER NOT NULL DEFAULT 0,
	blob_id INTEGER REFERENCES blobs(id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_files_path_version ON files (path, version DESC);
CREATE INDEX IF NOT EXISTS idx_files_blob_id ON files (blob_id) WHERE blob_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_files_created_at ON files (created_at);

CREATE TRIGGER IF NOT EXISTS files_immutable
BEFORE UPDATE ON files
BEGIN
	SELECT RAISE(ABORT, 'files rows are immutable');
END;

CREATE TRIGGER IF NOT EXISTS blobs_mark_unreferenced
AFTER INSERT ON files
BEGIN
	UPDATE blobs SET local_deleting = 1
	WHERE local_written = 1
	AND local_deleting = 0
	AND remote_written = 1
	AND id NOT IN (
		SELECT blob_id FROM files f1
		WHERE blob_id IS NOT NULL
		AND f1.version = (SELECT MAX(f2.version) FROM files f2 WHERE f2.path = f1.path)
		AND f1.deleted = 0
	);
END;

CREATE TRIGGER IF NOT EXISTS blobs_mark_remote_deleting
AFTER DELETE ON files
BEGIN
	UPDATE blobs SET remote_deleting = 1
	WHERE remote_written = 1
	AND remote_deleting = 0
	AND id NOT IN (
		SELECT blob_id FROM files WHERE blob_id IS NOT NULL
	);
END;

CREATE TRIGGER IF NOT EXISTS blobs_delete_fully_deleted
AFTER UPDATE ON blobs
WHEN NEW.local_deleted = 1 AND NEW.remote_deleted = 1
BEGIN
	DELETE FROM blobs WHERE id = NEW.id;
END;
