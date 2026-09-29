package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestFileStore_LockSerialization(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	unlock, err := s.TryLock(context.Background(), "L1")
	if err != nil {
		t.Fatalf("first TryLock: %v", err)
	}
	_, err = s.TryLock(context.Background(), "L1")
	if !errors.Is(err, ErrInstanceActive) {
		t.Fatalf("second TryLock err=%v want ErrInstanceActive", err)
	}
	unlock()
	unlock2, err := s.TryLock(context.Background(), "L1")
	if err != nil {
		t.Fatalf("TryLock after unlock: %v", err)
	}
	unlock2()
}

// TestFileStore_IndependentHandlesCrossProcess simulates two "processes"
// using independent store handles over the same directory (flock works on
// file descriptions, so this exercises the cross-process path).
func TestFileStore_IndependentHandlesCrossProcess(t *testing.T) {
	dir := t.TempDir()
	s1, _ := NewFileStore(dir)
	s2, _ := NewFileStore(dir)

	u1, err := s1.TryLock(context.Background(), "XPROC")
	if err != nil {
		t.Fatalf("s1 lock: %v", err)
	}
	_, err = s2.TryLock(context.Background(), "XPROC")
	if !errors.Is(err, ErrInstanceActive) {
		t.Fatalf("independent handle lock err=%v want ErrInstanceActive (cross-process flock)", err)
	}
	u1()
	u2, err := s2.TryLock(context.Background(), "XPROC")
	if err != nil {
		t.Fatalf("s2 lock after s1 unlock: %v", err)
	}
	u2()
}

func TestFileStore_LoadMissing(t *testing.T) {
	s, _ := NewFileStore(t.TempDir())
	_, err := s.Load(context.Background(), "nope")
	if !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("err=%v want ErrInstanceNotFound", err)
	}
}

func TestFileStore_CreateThenCreateRejected(t *testing.T) {
	s, _ := NewFileStore(t.TempDir())
	inst := newInstance(specWith(task("a", "h")), "dup-id")
	if err := s.Create(context.Background(), inst); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := s.Create(context.Background(), inst); !errors.Is(err, ErrInstanceActive) {
		t.Fatalf("second create err=%v want ErrInstanceActive", err)
	}
}

func TestFileStore_UnsafeInstanceID(t *testing.T) {
	s, _ := NewFileStore(t.TempDir())
	for _, bad := range []string{"../escape", "a/b", "..", ""} {
		_, err := s.Load(context.Background(), bad)
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("id=%q err=%v want ErrInvalidConfig", bad, err)
		}
	}
}

func TestFileStore_ConcurrentSaves(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewFileStore(dir)
	inst := newInstance(specWith(task("a", "h")), "conc-save")
	if err := s.Create(context.Background(), inst); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, err := s.Load(context.Background(), inst.ID)
			if err != nil {
				t.Error(err)
				return
			}
			got.Tasks["a"].Error = fmt.Sprintf("e%d", i)
			if err := s.Save(context.Background(), got); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	got, err := s.Load(context.Background(), inst.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Tasks["a"].Error == "" {
		t.Fatal("expected some error to be persisted")
	}
}
