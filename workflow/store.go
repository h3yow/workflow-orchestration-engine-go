package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// Store durably persists workflow instances. Implementations must provide
// atomic, crash-safe writes (temp file + fsync + rename) and must serialize
// concurrent access to the same instance.
type Store interface {
	// Create persists a brand-new instance. It returns ErrInstanceActive if
	// the instance ID already exists.
	Create(ctx context.Context, inst *Instance) error
	// Load reads an instance by ID (ErrInstanceNotFound when absent).
	Load(ctx context.Context, id string) (*Instance, error)
	// Save atomically overwrites an instance's state.
	Save(ctx context.Context, inst *Instance) error
	// Lock acquires an exclusive advisory lock for the instance and returns
	// an unlock function. It blocks until the lock is acquired or ctx ends.
	Lock(ctx context.Context, id string) (unlock func(), err error)
	// TryLock is the non-blocking variant: it returns ErrInstanceActive when
	// the instance lock is already held (in-process or by another process).
	TryLock(ctx context.Context, id string) (unlock func(), err error)
	// List returns IDs of all persisted instances.
	List(ctx context.Context) ([]string, error)
}

// FileStore is a directory-backed, deterministic local Store. Each instance
// is one JSON file; writes are temp-file + fsync + rename. Two independent
// lock layers exist:
//   - runLocks: in-process mutex guarding a whole scheduler run (paired with
//     a cross-process flock on the <id>.lock sidecar);
//   - ioLocks:  per-file mutex serializing Create/Load/Save read-modify-write
//     sequences. It is distinct from runLocks, so a run holding its lock can
//     still persist state without self-deadlocking.
type FileStore struct {
	dir string

	runMu sync.Mutex
	runs  map[string]*sync.Mutex

	ioMu sync.Mutex
	ios  map[string]*sync.Mutex
}

// NewFileStore opens (creating if needed) the state directory.
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: state dir is empty", ErrInvalidConfig)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("%w: cannot create state dir %q: %v", ErrInvalidConfig, dir, err)
	}
	// Fail fast if the directory is not usable.
	f, err := os.CreateTemp(dir, ".probe-*")
	if err != nil {
		return nil, fmt.Errorf("%w: state dir %q not writable: %v", ErrInvalidConfig, dir, err)
	}
	name := f.Name()
	f.Close()
	os.Remove(name)
	return &FileStore{dir: dir, runs: make(map[string]*sync.Mutex), ios: make(map[string]*sync.Mutex)}, nil
}

func (s *FileStore) path(id string) (string, error) {
	if id == "" {
		return "", fmt.Errorf("%w: empty instance id", ErrInvalidConfig)
	}
	if strings.ContainsAny(id, string(filepath.Separator)+"/\\") || id == "." || id == ".." {
		return "", fmt.Errorf("%w: unsafe instance id %q", ErrInvalidConfig, id)
	}
	return filepath.Join(s.dir, id+".json"), nil
}

// entryMu returns (creating if needed) the mutex stored in m[key].
func entryMu(mu *sync.Mutex, m map[string]*sync.Mutex, key string) *sync.Mutex {
	mu.Lock()
	defer mu.Unlock()
	mt, ok := m[key]
	if !ok {
		mt = &sync.Mutex{}
		m[key] = mt
	}
	return mt
}

func (s *FileStore) runMutex(id string) *sync.Mutex { return entryMu(&s.runMu, s.runs, id) }
func (s *FileStore) ioMutex(id string) *sync.Mutex  { return entryMu(&s.ioMu, s.ios, id) }

// Create refuses to overwrite an existing instance. Detection is atomic via
// hard link: temp -> final succeeds only if final does not yet exist.
func (s *FileStore) Create(ctx context.Context, inst *Instance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := s.path(inst.ID)
	if err != nil {
		return err
	}
	io := s.ioMutex(inst.ID)
	io.Lock()
	defer io.Unlock()

	data, err := json.MarshalIndent(inst, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: encode failed: %v", ErrInvalidState, err)
	}
	tmp, err := os.CreateTemp(s.dir, ".create-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// Atomic "create only if absent": link fails with EEXIST.
	if err := os.Link(tmpName, p); err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("%w: instance %q", ErrInstanceActive, inst.ID)
		}
		return err
	}
	if df, err := os.Open(s.dir); err == nil {
		_ = df.Sync()
		df.Close()
	}
	return nil
}

func (s *FileStore) Load(ctx context.Context, id string) (*Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, err := s.path(id)
	if err != nil {
		return nil, err
	}
	io := s.ioMutex(id)
	io.Lock()
	defer io.Unlock()
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %q", ErrInstanceNotFound, id)
		}
		return nil, err
	}
	inst := &Instance{}
	if err := json.Unmarshal(data, inst); err != nil {
		return nil, fmt.Errorf("%w: cannot decode %q: %v", ErrInvalidState, id, err)
	}
	if inst.Tasks == nil || inst.ID == "" {
		return nil, fmt.Errorf("%w: instance %q missing fields", ErrInvalidState, id)
	}
	return inst, nil
}

func (s *FileStore) Save(ctx context.Context, inst *Instance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := s.path(inst.ID)
	if err != nil {
		return err
	}
	io := s.ioMutex(inst.ID)
	io.Lock()
	defer io.Unlock()
	return s.writeFile(p, inst)
}

// writeFile performs an atomic, durable replace:
// write temp in same dir -> fsync temp -> rename -> fsync dir.
func (s *FileStore) writeFile(p string, inst *Instance) error {
	data, err := json.MarshalIndent(inst, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: encode failed: %v", ErrInvalidState, err)
	}
	dir := filepath.Dir(p)
	tmp, err := os.CreateTemp(dir, ".tmp-"+filepath.Base(p)+"-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, p); err != nil {
		return err
	}
	cleanup = false
	// Best-effort fsync of the directory so the rename itself is durable.
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		df.Close()
	}
	return nil
}

// Lock takes the in-process mutex first, then the cross-process flock on
// <id>.lock. Returning while holding the in-process mutex means a second
// goroutine in this process blocks even if flock would be reentrant-capable
// (it is not per-goroutine, but serialization is still guaranteed).
func (s *FileStore) Lock(ctx context.Context, id string) (func(), error) {
	return s.lock(ctx, id, false)
}

// TryLock is the non-blocking counterpart: an already-held lock (in-process
// TryLock fails fast or cross-process flock returns EWOULDBLOCK) yields
// ErrInstanceActive instead of waiting.
func (s *FileStore) TryLock(ctx context.Context, id string) (func(), error) {
	return s.lock(ctx, id, true)
}

func (s *FileStore) lock(ctx context.Context, id string, nonBlocking bool) (func(), error) {
	p, err := s.path(id)
	if err != nil {
		return nil, err
	}
	m := s.runMutex(id)
	if nonBlocking {
		if !m.TryLock() {
			return nil, fmt.Errorf("%w: instance %q is running in this process", ErrInstanceActive, id)
		}
	} else {
		m.Lock()
	}
	lockPath := p + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		m.Unlock()
		return nil, err
	}
	if nonBlocking {
		if err := tryFlock(f); err != nil {
			f.Close()
			m.Unlock()
			if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
				return nil, fmt.Errorf("%w: instance %q is locked by another process", ErrInstanceActive, id)
			}
			return nil, err
		}
		return s.unlockFunc(f, m, lockPath), nil
	}
	acquired := make(chan error, 1)
	go func() { acquired <- flock(f) }()
	select {
	case err := <-acquired:
		if err != nil {
			f.Close()
			m.Unlock()
			return nil, err
		}
		return s.unlockFunc(f, m, lockPath), nil
	case <-ctx.Done():
		// Can't cancel flock itself; abandon waiter goroutine (it finishes
		// once the lock is free) and bail out.
		go func() {
			if err := <-acquired; err == nil {
				_ = funlock(f)
				f.Close()
				m.Unlock()
			}
		}()
		return nil, ctx.Err()
	}
}

// unlockFunc releases the flock, closes the descriptor, removes the sidecar
// (best effort; it is safe to leave behind) and releases the in-process mutex.
func (s *FileStore) unlockFunc(f *os.File, m *sync.Mutex, lockPath string) func() {
	return func() {
		_ = funlock(f)
		f.Close()
		_ = os.Remove(lockPath)
		m.Unlock()
	}
}

func (s *FileStore) List(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".") {
			continue
		}
		ids = append(ids, strings.TrimSuffix(name, ".json"))
	}
	return ids, nil
}

// guard so errors import is retained even if future edits drop a use.
var _ = errors.Is
