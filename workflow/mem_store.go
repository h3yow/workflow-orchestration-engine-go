package workflow

import (
	"context"
	"sort"
	"sync"
)

// memStore 进程内状态存储，仅供测试（进程重启即丢失）。
type memStore struct {
	mu sync.Mutex
	m  map[string]*State
}

func newMemStore() *memStore {
	return &memStore{m: map[string]*State{}}
}

func (s *memStore) Load(_ context.Context, id string) (*State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.m[id]
	if !ok {
		return nil, nil
	}
	return cloneState(st), nil
}

func (s *memStore) Save(_ context.Context, st *State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[st.WorkflowID] = cloneState(st)
	return nil
}

func (s *memStore) List(_ context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.m))
	for id := range s.m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}
