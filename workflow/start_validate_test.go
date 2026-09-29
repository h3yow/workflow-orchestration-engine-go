package workflow

import (
	"context"
	"testing"
)

func TestStartRejectsInvalidDefinition(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)

	bad := &Definition{Name: "bad", Tasks: []TaskDefinition{
		{ID: "a", Handler: "h"},
		{ID: "b", Handler: "h", DependsOn: []string{"a"}},
		{ID: "a", Handler: "h"}, // duplicate
	}}
	_, err := e.Start(context.Background(), bad, "wf-bad")
	de, ok := err.(*DefinitionError)
	if !ok || de.Code != ErrCodeDuplicateID {
		t.Fatalf("want duplicate_task_id, got %v", err)
	}
}

func TestStartRejectsUnregisteredHandler(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	def := &Definition{Name: "x", Tasks: []TaskDefinition{{ID: "a", Handler: "missing"}}}
	_, err := e.Start(context.Background(), def, "wf-x")
	de, ok := err.(*DefinitionError)
	if !ok || de.Code != ErrCodeInvalidConfig {
		t.Fatalf("want invalid_config for missing handler, got %v", err)
	}
}

func TestStartRejectsUnregisteredCondition(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	_ = e.RegisterHandler("h", func(ec *ExecContext) (map[string]any, error) { return nil, nil })
	def := &Definition{Name: "x", Tasks: []TaskDefinition{
		{ID: "a", Handler: "h"},
		{ID: "b", Handler: "h", DependsOn: []string{"a"}, Condition: "cond:nope"},
	}}
	_, err := e.Start(context.Background(), def, "wf-x2")
	de, ok := err.(*DefinitionError)
	if !ok || de.Code != ErrCodeInvalidConfig {
		t.Fatalf("want invalid_config for missing condition, got %v", err)
	}
}

func TestRegisterDuplicateRejected(t *testing.T) {
	e, _ := NewEngine(Config{})
	h := func(ec *ExecContext) (map[string]any, error) { return nil, nil }
	if err := e.RegisterHandler("h", h); err != nil {
		t.Fatal(err)
	}
	if err := e.RegisterHandler("h", h); err == nil {
		t.Fatal("duplicate handler registration should fail")
	}
	if err := e.RegisterHandler("", h); err == nil {
		t.Fatal("empty handler name should fail")
	}
	if err := e.RegisterHandler("nil", nil); err == nil {
		t.Fatal("nil handler should fail")
	}
}
