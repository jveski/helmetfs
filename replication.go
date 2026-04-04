package main

import (
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

func replWorkerLoop(state *FsState) {
	defer state.replWg.Done()
	for !state.shutdown.Load() {
		work := state.replLog.DequeueNext()
		if work == nil {
			break
		}
		backoff := time.Second
		maxBackoff := 300 * time.Second
		for !state.shutdown.Load() {
			var err error
			switch work.op {
			case ReplPut:
				err = replicatePut(state, work.path)
			case ReplDelete:
				err = replicateDelete(state, work.path)
			}
			if err == nil {
				state.replLog.MarkCompleted(work.id)
				break
			}
			log.Printf("replication error for %s: %v", work.path, err)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func replicatePut(state *FsState, relPath string) error {
	backingPath := filepath.Join(state.backingDir, relPath)
	replicaPath := filepath.Join(state.replicaDir, "files", relPath)

	var st syscall.Stat_t
	err := syscall.Lstat(backingPath, &st)
	if err != nil {
		if os.IsNotExist(err) || err == syscall.ENOENT {
			return nil
		}
		return err
	}

	if st.Mode&syscall.S_IFMT == syscall.S_IFLNK {
		if err := ensureParentDir(replicaPath); err != nil {
			return err
		}
		os.Remove(replicaPath)
		target, err := os.Readlink(backingPath)
		if err != nil {
			return err
		}
		return os.Symlink(target, replicaPath)
	}

	storedHex, storedErr := readSumFile(backingPath + ".sum")
	computedHex, computedErr := computeBlake3(backingPath)
	if storedErr == nil && computedErr == nil {
		if storedHex != computedHex {
			log.Printf("integrity mismatch for %s, skipping replication", relPath)
			return nil
		}
	}

	if err := ensureParentDir(replicaPath); err != nil {
		return err
	}
	if err := copyFileWithSync(backingPath, replicaPath); err != nil {
		return err
	}

	sumBacking := backingPath + ".sum"
	sumReplica := replicaPath + ".sum"
	copyFileWithSync(sumBacking, sumReplica)

	err = syscall.Stat(backingPath, &st)
	if err != nil {
		return nil
	}
	os.Chmod(replicaPath, os.FileMode(st.Mode&0o7777))
	os.Chown(replicaPath, int(st.Uid), int(st.Gid))
	return nil
}

func replicateDelete(state *FsState, relPath string) error {
	replicaPath := filepath.Join(state.replicaDir, "files", relPath)
	os.Remove(replicaPath)
	os.Remove(replicaPath + ".sum")
	if !state.noRemoteMkdir {
		removeEmptyParentDirs(replicaPath, filepath.Join(state.replicaDir, "files"))
	}
	return nil
}
