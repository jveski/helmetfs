package main

import "sync"

type PathInfo struct {
	dirtyGen    uint64
	cleanGen    uint64
	writeRefcnt uint32
}

type PathStateMap struct {
	mu sync.RWMutex
	m  map[string]*PathInfo
}

func NewPathStateMap() *PathStateMap {
	return &PathStateMap{m: make(map[string]*PathInfo)}
}

func (ps *PathStateMap) SetDirty(path string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	info, ok := ps.m[path]
	if !ok {
		info = &PathInfo{}
		ps.m[path] = info
	}
	info.dirtyGen++
}

func (ps *PathStateMap) IsDirty(path string) bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	info, ok := ps.m[path]
	if !ok {
		return false
	}
	return info.dirtyGen > info.cleanGen
}

func (ps *PathStateMap) ClearDirty(path string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	info, ok := ps.m[path]
	if !ok {
		return
	}
	info.cleanGen = info.dirtyGen
}

func (ps *PathStateMap) ClearDirtyIfGen(path string, gen uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	info, ok := ps.m[path]
	if !ok {
		return
	}
	if info.dirtyGen == gen {
		info.cleanGen = gen
	}
}

func (ps *PathStateMap) GetDirtyGen(path string) uint64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	info, ok := ps.m[path]
	if !ok {
		return 0
	}
	return info.dirtyGen
}

func (ps *PathStateMap) IncWriteRef(path string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	info, ok := ps.m[path]
	if !ok {
		info = &PathInfo{}
		ps.m[path] = info
	}
	info.writeRefcnt++
}

func (ps *PathStateMap) DecWriteRef(path string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	info, ok := ps.m[path]
	if !ok {
		return
	}
	if info.writeRefcnt > 0 {
		info.writeRefcnt--
	}
}

func (ps *PathStateMap) HasWriteRef(path string) bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	info, ok := ps.m[path]
	if !ok {
		return false
	}
	return info.writeRefcnt > 0
}

func (ps *PathStateMap) Remove(path string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	delete(ps.m, path)
}

func (ps *PathStateMap) CollectDirtyPaths() []string {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	var paths []string
	for p, info := range ps.m {
		if info.dirtyGen > info.cleanGen {
			paths = append(paths, p)
		}
	}
	return paths
}
