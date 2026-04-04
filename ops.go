package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func checksumAndEnqueue(state *FsState, relPath string) error {
	if state.pathState.HasWriteRef(relPath) {
		return nil
	}
	return checksumAndEnqueueForced(state, relPath)
}

func checksumAndEnqueueForced(state *FsState, relPath string) error {
	gen := state.pathState.GetDirtyGen(relPath)
	backingPath := filepath.Join(state.backingDir, relPath)
	hexDigest, err := computeBlake3(backingPath)
	if err != nil {
		return fmt.Errorf("checksum %s: %w", relPath, err)
	}
	sumPath := backingPath + ".sum"
	if err := writeSumFile(sumPath, hexDigest); err != nil {
		return fmt.Errorf("write sum %s: %w", relPath, err)
	}
	state.replLog.Enqueue(ReplPut, relPath)
	state.pathState.ClearDirtyIfGen(relPath, gen)
	return nil
}

func flushDirtyFiles(state *FsState) {
	paths := state.pathState.CollectDirtyPaths()
	for _, p := range paths {
		checksumAndEnqueue(state, p)
	}
}

func stopWorkers(state *FsState) {
	state.shutdown.Store(true)
	state.replLog.Broadcast()
	state.replWg.Wait()
	state.scrubWg.Wait()
}

func initState(state *FsState) {
	os.MkdirAll(filepath.Join(state.backingDir, ".helmetfs"), 0755)
}
