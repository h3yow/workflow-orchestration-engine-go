package workflow

import (
	"context"
	"errors"
	"testing"
)

func regWith(handlers ...string) *Registry {
	r := NewRegistry()
	for _, h := range handlers {
		r.MustRegister(h, func(_ context.Context, _ TaskInput) (TaskResult, error) {
			return TaskResult{}, nil
		})
	}
	return r
}

func specWith(tasks ...TaskSpec) WorkflowSpec {
	return WorkflowSpec{Name: "wf", MaxParallel: 1, Tasks: tasks}
}

func task(id, handler string, deps ...string) TaskSpec {
	return TaskSpec{ID: id, Handler: handler, DependsOn: deps}
}

func TestValidate_RejectInvalidDefinitions(t *testing.T) {
	reg := regWith("h")

	cases := []struct {
		name string
		spec WorkflowSpec
		want error
	}{
		{
			name: "empty workflow name",
			spec: WorkflowSpec{Tasks: []TaskSpec{task("a", "h")}},
			want: ErrInvalidConfig,
		},
		{
			name: "no tasks",
			spec: specWith(),
			want: ErrInvalidConfig,
		},
		{
			name: "empty task id",
			spec: specWith(TaskSpec{Handler: "h"}),
			want: ErrInvalidConfig,
		},
		{
			name: "empty handler",
			spec: specWith(TaskSpec{ID: "a"}),
			want: ErrInvalidConfig,
		},
		{
			name: "duplicate task id",
			spec: specWith(task("a", "h"), task("a", "h")),
			want: ErrDuplicateTaskID,
		},
		{
			name: "missing dependency",
			spec: specWith(task("a", "h", "ghost")),
			want: ErrMissingDependency,
		},
		{
			name: "self dependency",
			spec: specWith(func() TaskSpec { x := task("a", "h", "a"); return x }()),
			want: ErrSelfDependency,
		},
		{
			name: "duplicate dependency listed twice",
			spec: specWith(task("a", "h"), task("b", "h", "a", "a")),
			want: ErrInvalidConfig,
		},
		{
			name: "cyclic dependency a->b->a",
			spec: specWith(task("a", "h", "b"), task("b", "h", "a")),
			want: ErrCyclicDependency,
		},
		{
			name: "cyclic dependency three nodes",
			spec: specWith(
				task("a", "h", "c"),
				task("b", "h", "a"),
				task("c", "h", "b"),
			),
			want: ErrCyclicDependency,
		},
		{
			name: "unknown handler",
			spec: specWith(task("a", "nope")),
			want: ErrUnknownHandler,
		},
		{
			name: "negative max parallel",
			spec: WorkflowSpec{Name: "wf", MaxParallel: -1, Tasks: []TaskSpec{task("a", "h")}},
			want: ErrInvalidConfig,
		},
		{
			name: "bad condition operator",
			spec: specWith(TaskSpec{ID: "a", Handler: "h", Condition: &Condition{Op: "??", Ref: "a"}}),
			want: ErrInvalidConfig,
		},
		{
			name: "negative retry attempts",
			spec: specWith(TaskSpec{ID: "a", Handler: "h", Retry: RetryPolicy{MaxAttempts: -1}}),
			want: ErrInvalidConfig,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.spec, reg)
			if err == nil {
				t.Fatalf("expected error wrapping %v, got nil", tc.want)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error %v does not wrap %v", err, tc.want)
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("expected *ValidationError, got %T: %v", err, err)
			}
			t.Logf("rejected as expected: %v (task=%s)", ve.Error(), ve.TaskID)
		})
	}
}

func TestValidate_AcceptsValidDAG(t *testing.T) {
	reg := regWith("h")
	spec := specWith(
		task("a", "h"),
		task("b", "h", "a"),
		task("c", "h", "a"),
		task("d", "h", "b", "c"),
	)
	if err := Validate(spec, reg); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	order := topoSort(spec)
	t.Logf("topo order: %v", order)
	pos := map[string]int{}
	for i, id := range order {
		pos[id] = i
	}
	if !(pos["a"] < pos["b"] && pos["a"] < pos["c"] && pos["b"] < pos["d"] && pos["c"] < pos["d"]) {
		t.Fatalf("topo order violates dependencies: %v", order)
	}
}

func TestCondition_Evaluate(t *testing.T) {
	outputs := map[string]any{
		"build": map[string]any{"ok": true, "n": float64(3), "name": "app"},
		"empty": map[string]any{"flag": false},
	}
	cases := []struct {
		c    Condition
		want bool
	}{
		{Condition{Op: "truthy", Ref: "build.ok"}, true},
		{Condition{Op: "truthy", Ref: "empty.flag"}, false},
		{Condition{Op: "falsy", Ref: "empty.flag"}, true},
		{Condition{Op: "exists", Ref: "build.n"}, true},
		{Condition{Op: "exists", Ref: "build.missing"}, false},
		{Condition{Op: "eq", Ref: "build.n", Value: 3}, true},
		{Condition{Op: "eq", Ref: "build.name", Value: "app"}, true},
		{Condition{Op: "ne", Ref: "build.name", Value: "other"}, true},
		{Condition{Op: "eq", Ref: "build", Value: nil}, false},
	}
	for _, tc := range cases {
		got, err := tc.c.Evaluate(outputs)
		if err != nil {
			t.Fatalf("condition %+v: %v", tc.c, err)
		}
		if got != tc.want {
			t.Fatalf("condition %+v = %v, want %v", tc.c, got, tc.want)
		}
	}
}
