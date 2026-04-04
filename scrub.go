package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func scrubLoop(state *FsState) {
	defer state.scrubWg.Done()
	if shouldScrubImmediately(state) {
		runScrub(state)
	}
	for !state.shutdown.Load() {
		ns := nsUntilNextScrub(state.scrubHour, state.scrubMinute)
		sleepWithShutdown(state, time.Duration(ns))
		if !state.shutdown.Load() {
			runScrub(state)
		}
	}
}

func sleepWithShutdown(state *FsState, d time.Duration) {
	end := time.Now().Add(d)
	for time.Now().Before(end) && !state.shutdown.Load() {
		remaining := time.Until(end)
		if remaining > time.Second {
			remaining = time.Second
		}
		time.Sleep(remaining)
	}
}

func shouldScrubImmediately(state *FsState) bool {
	tsPath := filepath.Join(state.backingDir, ".helmetfs", "scrub.timestamp")
	data, err := os.ReadFile(tsPath)
	if err != nil {
		return true
	}
	s := strings.TrimSpace(string(data))
	ts, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return true
	}
	return time.Now().Unix()-ts > 86400
}

func nsUntilNextScrub(targetHour, targetMinute int) uint64 {
	now := time.Now()
	nowSecs := now.Hour()*3600 + now.Minute()*60 + now.Second()
	targetSecs := targetHour*3600 + targetMinute*60
	var delta int
	if targetSecs > nowSecs {
		delta = targetSecs - nowSecs
	} else {
		delta = 86400 - nowSecs + targetSecs
	}
	if delta == 0 {
		delta = 86400
	}
	return uint64(delta) * 1_000_000_000
}

func runScrub(state *FsState) {
	log.Println("scrub: starting")
	var corruptions, repairs int

	err := filepath.Walk(state.backingDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if state.shutdown.Load() {
			return filepath.SkipAll
		}
		relPath, _ := filepath.Rel(state.backingDir, path)
		if relPath == "." {
			return nil
		}
		if info.IsDir() {
			if strings.HasPrefix(relPath, ".helmetfs") {
				return filepath.SkipDir
			}
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if strings.HasPrefix(relPath, ".helmetfs") {
			return nil
		}
		if strings.HasSuffix(relPath, ".sum") {
			return nil
		}
		if state.pathState.HasWriteRef(relPath) {
			return nil
		}
		scrubFile(state, relPath, &corruptions, &repairs)
		return nil
	})
	if err != nil {
		log.Printf("scrub: walk error: %v", err)
	}
	writeScrubTimestamp(state, time.Now().Unix())
	log.Printf("scrub: completed, corruptions=%d repairs=%d", corruptions, repairs)
}

func scrubFile(state *FsState, relPath string, corruptions, repairs *int) {
	backingPath := filepath.Join(state.backingDir, relPath)
	currentHex, err := computeBlake3(backingPath)
	if err != nil {
		log.Printf("scrub: checksum error for %s: %v", relPath, err)
		return
	}
	storedHex, err := readSumFile(backingPath + ".sum")
	if err != nil {
		if os.IsNotExist(err) {
			writeSumFile(backingPath+".sum", currentHex)
			state.replLog.Enqueue(ReplPut, relPath)
			return
		}
		log.Printf("scrub: read sum error for %s: %v", relPath, err)
		return
	}

	if currentHex == storedHex {
		return
	}

	*corruptions++
	log.Printf("scrub: corruption detected in %s", relPath)

	hasPending := state.replLog.HasPendingPut(relPath)
	replicaPath := filepath.Join(state.replicaDir, "files", relPath)
	replicaHex, err := readSumFile(replicaPath + ".sum")
	if err != nil {
		log.Printf("scrub: cannot read replica sum for %s: %v", relPath, err)
		return
	}

	replicaComputed, err := computeBlake3(replicaPath)
	if err != nil {
		log.Printf("scrub: cannot compute replica hash for %s: %v", relPath, err)
		return
	}

	if replicaComputed != replicaHex {
		log.Printf("scrub: replica also corrupt for %s", relPath)
		return
	}

	if hasPending {
		log.Printf("scrub: replica is stale for %s, skipping repair", relPath)
		return
	}

	if state.pathState.HasWriteRef(relPath) {
		return
	}
	if state.pathState.IsDirty(relPath) {
		return
	}

	if err := copyFileWithSync(replicaPath, backingPath); err != nil {
		log.Printf("scrub: repair failed for %s: %v", relPath, err)
		return
	}
	writeSumFile(backingPath+".sum", replicaComputed)
	*repairs++
	log.Printf("scrub: repaired %s", relPath)
}

func writeScrubTimestamp(state *FsState, ts int64) {
	tsPath := filepath.Join(state.backingDir, ".helmetfs", "scrub.timestamp")
	f, err := os.Create(tsPath)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "%d\n", ts)
	f.Sync()
}
