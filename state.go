package main

import (
	"sync"
	"sync/atomic"
)

type AtomicBool struct {
	v atomic.Bool
}

func (a *AtomicBool) Store(val bool) { a.v.Store(val) }
func (a *AtomicBool) Load() bool     { return a.v.Load() }

type FsState struct {
	backingDir  string
	replicaDir  string
	scrubHour   int
	scrubMinute int
	replWorkers int
	noRemoteMkdir bool
	pathState   *PathStateMap
	replLog     *ReplLog
	shutdown    AtomicBool
	scrubWg     sync.WaitGroup
	replWg      sync.WaitGroup
}

func NewFsState(backingDir, replicaDir string, scrubHour, scrubMinute, replWorkers int, noRemoteMkdir bool) *FsState {
	s := &FsState{
		backingDir:    backingDir,
		replicaDir:    replicaDir,
		scrubHour:     scrubHour,
		scrubMinute:   scrubMinute,
		replWorkers:   replWorkers,
		noRemoteMkdir: noRemoteMkdir,
		pathState:     NewPathStateMap(),
	}
	s.replLog = NewReplLog(backingDir, &s.shutdown)
	return s
}
