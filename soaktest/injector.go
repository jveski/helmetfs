package main

import (
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
)

// Injector provides failure injection for testing resilience.
type Injector struct {
	blobsDir    string
	corruptRate float64
}

// CorruptRandomBlob corrupts a random blob file by flipping bits.
func (inj *Injector) CorruptRandomBlob(rng *rand.Rand) {
	if inj == nil || inj.blobsDir == "" {
		return
	}

	// Find a random blob file
	entries, err := os.ReadDir(inj.blobsDir)
	if err != nil || len(entries) == 0 {
		return
	}

	// Pick a random subdirectory
	subdir := entries[rng.IntN(len(entries))]
	if !subdir.IsDir() {
		return
	}

	subdirPath := filepath.Join(inj.blobsDir, subdir.Name())
	files, err := os.ReadDir(subdirPath)
	if err != nil || len(files) == 0 {
		return
	}

	// Pick a random file
	file := files[rng.IntN(len(files))]
	if file.IsDir() {
		return
	}

	blobPath := filepath.Join(subdirPath, file.Name())
	info, err := file.Info()
	if err != nil || info.Size() == 0 {
		return
	}

	// Corrupt it
	f, err := os.OpenFile(blobPath, os.O_RDWR, 0)
	if err != nil {
		return
	}
	defer f.Close()

	// Flip 1-3 random bits
	numFlips := 1 + rng.IntN(3)
	for i := 0; i < numFlips; i++ {
		offset := rng.Int64N(info.Size())
		var b [1]byte
		if _, err := f.ReadAt(b[:], offset); err != nil {
			return
		}
		b[0] ^= byte(1 << rng.IntN(8))
		if _, err := f.WriteAt(b[:], offset); err != nil {
			return
		}
	}

	log.Printf("[injector] corrupted blob %s (%d bit flips)", blobPath, numFlips)
}
