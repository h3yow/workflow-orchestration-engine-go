// Package workflow implements a local, deterministic workflow orchestration
// engine: dependency-based scheduling, conditional branches, dynamic task
// spawning, bounded concurrency, timeout/cancellation, retries, and durable
// state that survives process restarts.
package workflow

import (
	"context"
	"time"
)

// ---------------------------------------------------------------------------
// Status enumerations
// ---------------------------------------------------------------------------

// TaskStatus is the lifecycle status of a single task within an instance.
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"   // waiting for dependencies / a worker slot
	TaskReady     TaskStatus = "ready"     // dependencies satisfied, eligible to run
	TaskRunning   TaskStatus = "running"   // a worker is executing the task
	TaskSucceeded TaskStatus = "succeeded" // handler returned nil
	TaskFailed    TaskStatus = "failed"    // retries exhausted, or non-retryable failure
	TaskSkipped   TaskStatus = "skipped"   // not run: dependency failed/skipped or condition false
	TaskCanceled  TaskStatus = "canceled"  // context canceled while pending/running
	TaskTimedOut  TaskStatus = "timed_out" // task-level deadline exceeded
)

// Terminal reports whether the task reached a terminal state.
// Note: skipped tasks are terminal but do not constitute success.
func (s TaskStatus) Terminal() bool {
	switch s {
	case TaskSucceeded, TaskFailed, TaskSkipped, TaskCanceled, TaskTimedOut:
		return true
	}
	return false
}

// InstanceStatus is the lifecycle status of a workflow instance.
type InstanceStatus string

const (
	InstancePending   InstanceStatus = "pending"   // persisted, not yet started
	InstanceRunning   InstanceStatus = "running"   // at least one task in flight
	InstanceSucceeded InstanceStatus = "succeeded" // all tasks succeeded (or legitimately skipped)
	InstanceFailed    InstanceStatus = "failed"    // one or more tasks failed/timed out
	InstanceCanceled  InstanceStatus = "canceled"  // canceled before completion
)

// Terminal reports whether the instance reached a terminal state.
func (s InstanceStatus) Terminal() bool {
	switch s {
	case InstanceSucceeded, InstanceFailed, InstanceCanceled:
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Task handler contract
// ---------------------------------------------------------------------------

// TaskInput is what a handler receives.
type TaskInput struct {
	InstanceID string
	TaskID     string
	Attempt    int            // 1-based
	Params     map[string]any // static params from the TaskSpec
	Outputs    map[string]any // outputs of succeeded upstream tasks, keyed by task ID
}

// TaskResult is what a handler returns on success.
type TaskResult struct {
	Output any `json:"output"`
}

// TaskHandler is the user-supplied, deterministic local unit of work.
// Returning an error fails the attempt; honoring ctx allows cancellation
// and timeout to work promptly.
type TaskHandler func(ctx context.Context, in TaskInput) (TaskResult, error)

// ---------------------------------------------------------------------------
// Definitions (immutable workflow specification)
// ---------------------------------------------------------------------------

// RetryPolicy describes attempt behavior for a task.
type RetryPolicy struct {
	MaxAttempts int           `json:"max_attempts"` // including the first attempt; <=1 means no retry
	Backoff     time.Duration `json:"backoff"`      // initial backoff
	MaxBackoff  time.Duration `json:"max_backoff"`  // backoff cap
	Factor      float64       `json:"factor"`       // backoff multiplier; <1 is treated as 1
	// Retryable decides whether an error is retryable. nil => all errors retryable.
}

// TaskSpec defines one task node in the workflow DAG.
type TaskSpec struct {
	ID        string         `json:"id"`
	Handler   string         `json:"handler"`
	Params    map[string]any `json:"params,omitempty"`
	DependsOn []string       `json:"depends_on,omitempty"`
	Timeout   time.Duration  `json:"timeout,omitempty"` // 0 => no task-level timeout
	Retry     RetryPolicy    `json:"retry"`
	Condition *Condition     `json:"condition,omitempty"`  // run only if condition holds
	AllowSkip bool           `json:"allow_skip,omitempty"` // if true, skipped deps do not fail this task
}

// WorkflowSpec is a complete, validated workflow definition.
type WorkflowSpec struct {
	Name        string     `json:"name"`
	Tasks       []TaskSpec `json:"tasks"`
	MaxParallel int        `json:"max_parallel"` // <=0 => 1
}

// Config holds engine-level knobs.
type Config struct {
	StateDir       string        // directory for durable state files
	MaxParallel    int           // default global concurrency when spec omits it
	TaskTimeout    time.Duration // default task timeout when spec omits it
	RecoveryResume bool          // on open, resume unfinished instances
}

// ---------------------------------------------------------------------------
// Runtime state (durable)
// ---------------------------------------------------------------------------

// Attempt records one execution attempt of a task, for traceability.
type Attempt struct {
	N         int           `json:"n"`
	StartedAt time.Time     `json:"started_at"`
	EndedAt   *time.Time    `json:"ended_at,omitempty"`
	Outcome   string        `json:"outcome"` // success | error | timeout | canceled
	Error     string        `json:"error,omitempty"`
	Backoff   time.Duration `json:"backoff,omitempty"` // delay before next attempt
}

// TaskState is the durable per-task runtime state.
type TaskState struct {
	ID       string     `json:"id"`
	Spec     TaskSpec   `json:"spec"`
	Status   TaskStatus `json:"status"`
	Attempts []Attempt  `json:"attempts,omitempty"`
	Output   any        `json:"output,omitempty"`
	Error    string     `json:"error,omitempty"`
	// DynamicTasks are tasks spawned at runtime by this task's handler.
	DynamicTasks []TaskSpec `json:"dynamic_tasks,omitempty"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

// Instance is the durable state of one workflow execution.
type Instance struct {
	ID       string                `json:"id"`
	SpecName string                `json:"spec_name"`
	Spec     WorkflowSpec          `json:"spec"`
	Status   InstanceStatus        `json:"status"`
	Tasks    map[string]*TaskState `json:"tasks"`
	// TopoOrder holds task IDs in dependency order (stable execution hint).
	TopoOrder []string  `json:"topo_order"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	// RunID changes on every Start/Resume; guards against duplicate concurrent runs.
	RunID string `json:"run_id"`
	Error string `json:"error,omitempty"`
}

// Result is a convenience constructor for TaskResult.
func Result(v any) TaskResult { return TaskResult{Output: v} }
