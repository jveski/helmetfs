package main

import (
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
)

type State struct {
	mu    sync.Mutex
	files map[string][]byte
	dirs  map[string]bool
}

func (s *State) addFile(path string, content []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[path] = content
}

func (s *State) removeFile(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files, path)
}

func (s *State) getFile(path string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.files[path]
	return c, ok
}

func (s *State) addDir(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirs[path] = true
}

func (s *State) removeDir(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.dirs, path)
}

func (s *State) hasDir(path string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirs[path]
}

func (s *State) randomFile(rng *rand.Rand) (string, []byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.files) == 0 {
		return "", nil, false
	}
	keys := slices.Collect(maps.Keys(s.files))
	p := keys[rng.IntN(len(keys))]
	return p, s.files[p], true
}

func (s *State) randomDir(rng *rand.Rand) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.dirs) == 0 {
		return "", false
	}
	keys := slices.Collect(maps.Keys(s.dirs))
	return keys[rng.IntN(len(keys))], true
}

func (s *State) listDir(dir string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var children []string
	prefix := dir
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if prefix == "//" {
		prefix = "/"
	}
	seen := make(map[string]bool)
	for p := range s.files {
		if strings.HasPrefix(p, prefix) {
			rest := strings.TrimPrefix(p, prefix)
			parts := strings.SplitN(rest, "/", 2)
			if !seen[parts[0]] {
				seen[parts[0]] = true
				children = append(children, parts[0])
			}
		}
	}
	for p := range s.dirs {
		if p != dir && strings.HasPrefix(p, prefix) {
			rest := strings.TrimPrefix(p, prefix)
			parts := strings.SplitN(rest, "/", 2)
			if !seen[parts[0]] {
				seen[parts[0]] = true
				children = append(children, parts[0])
			}
		}
	}
	return children
}
