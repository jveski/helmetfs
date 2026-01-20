package main

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

// FailureInjector enables testing corruption scenarios
type FailureInjector struct {
	db           *sql.DB
	blobsDir     string
	corruptRate  float64
	mu           sync.Mutex
	corruptedIDs map[int64]bool // tracks blob IDs we've corrupted
}

func newFailureInjector(dbPath, blobsDir string, corruptRate float64) (*FailureInjector, error) {
	if dbPath == "" && blobsDir == "" {
		return nil, nil // failure injection disabled
	}

	fi := &FailureInjector{
		blobsDir:     blobsDir,
		corruptRate:  corruptRate,
		corruptedIDs: make(map[int64]bool),
	}

	if dbPath != "" {
		db, err := sql.Open("sqlite3", "file:"+dbPath+"?_journal_mode=WAL&_busy_timeout=5000&mode=ro")
		if err != nil {
			return nil, fmt.Errorf("open database: %w", err)
		}
		fi.db = db
	}

	return fi, nil
}

func (fi *FailureInjector) Close() error {
	if fi != nil && fi.db != nil {
		return fi.db.Close()
	}
	return nil
}

// CorruptRandomBlob corrupts a random blob file by flipping bits
func (fi *FailureInjector) CorruptRandomBlob(rng *rand.Rand) error {
	if fi == nil || fi.blobsDir == "" || fi.db == nil {
		return nil
	}

	if rng.Float64() >= fi.corruptRate {
		return nil // skip based on rate
	}

	// Find a blob to corrupt
	var blobID int64
	var size int64
	err := fi.db.QueryRow(`
		SELECT id, size FROM blobs
		WHERE local_written = 1 AND local_deleted = 0 AND size > 0
		ORDER BY RANDOM() LIMIT 1`).Scan(&blobID, &size)
	if err == sql.ErrNoRows {
		return nil // no blobs to corrupt
	}
	if err != nil {
		return err
	}

	fi.mu.Lock()
	if fi.corruptedIDs[blobID] {
		fi.mu.Unlock()
		return nil // already corrupted
	}
	fi.corruptedIDs[blobID] = true
	fi.mu.Unlock()

	blobPath := blobFilePath(fi.blobsDir, blobID)
	return corruptFile(blobPath, rng, size)
}

func corruptFile(path string, rng *rand.Rand, size int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	// Flip 1-3 random bits in the file
	numFlips := 1 + rng.IntN(3)
	for i := 0; i < numFlips; i++ {
		offset := rng.Int64N(size)
		var b [1]byte
		if _, err := f.ReadAt(b[:], offset); err != nil {
			return err
		}
		bit := byte(1 << rng.IntN(8))
		b[0] ^= bit
		if _, err := f.WriteAt(b[:], offset); err != nil {
			return err
		}
	}

	log.Printf("[failure-injection] corrupted blob at %s (%d bit flips)", path, numFlips)
	return nil
}

// DeleteRandomLocalBlob deletes a local blob to simulate local data loss
func (fi *FailureInjector) DeleteRandomLocalBlob(rng *rand.Rand) error {
	if fi == nil || fi.blobsDir == "" || fi.db == nil {
		return nil
	}

	// Only do this occasionally
	if rng.Float64() >= fi.corruptRate {
		return nil
	}

	var blobID int64
	err := fi.db.QueryRow(`
		SELECT id FROM blobs
		WHERE local_written = 1 AND local_deleted = 0 AND remote_written = 1
		ORDER BY RANDOM() LIMIT 1`).Scan(&blobID)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}

	blobPath := blobFilePath(fi.blobsDir, blobID)
	if err := os.Remove(blobPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	log.Printf("[failure-injection] deleted local blob %d to simulate data loss", blobID)
	return nil
}

func blobFilePath(blobsDir string, blobID int64) string {
	h := fnv.New64a()
	binary.Write(h, binary.LittleEndian, blobID)
	hash := h.Sum64()
	return filepath.Join(blobsDir, fmt.Sprintf("%02x", hash&0xff), fmt.Sprintf("%014x", hash>>8))
}
