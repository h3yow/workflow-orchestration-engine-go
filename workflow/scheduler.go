package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// SpawnRequest is returned by a handler that wants to add tasks at runtime.
type SpawnRequest struct {
	Tasks []TaskSpec
}

// SpawnTasksError is a control-flow "error": the task itself succeeds (its
// output is taken from Result), but after it succeeds the listed tasks are
// validated and added to the instance DAG. Validation failure fails the
// spawning task atomically (nothing is added).
type SpawnTasksError struct {
	Result   TaskResult
	Requests []SpawnRequest
	Reason   string
}

func (e *SpawnTasksError) Error() string {
	if e == nil {
		return "<nil spawn>"
	}
	if e.Reason != "" {
		return "spawn tasks: " + e.Reason
	}
	return "spawn tasks"
}

// scheduler drives one run of one instance to completion.
type scheduler struct {
	inst           *Instance
	store          Store
	reg            *Registry
	log            Logger
	maxParallel    int
	defaultTimeout time.Duration

	mu     sync.Mutex // guards inst state read-modify-write
	active int        // workers currently executing a task attempt/backoff
	wake   chan struct{}
}

func newScheduler(inst *Instance, store Store, reg *Registry, log Logger, maxParallel int, defaultTimeout time.Duration) *scheduler {
	if maxParallel <= 0 {
		maxParallel = 1
	}
	return &scheduler{
		inst:           inst,
		store:          store,
		reg:            reg,
		log:            log,
		maxParallel:    maxParallel,
		defaultTimeout: defaultTimeout,
		wake:           make(chan struct{}, 1),
	}
}

func (s *scheduler) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// persist writes the current state. Callers must hold s.mu. It deliberately
// uses context.Background(): final state must reach disk even when the run's
// context has just been canceled.
func (s *scheduler) persistLocked(runCtx context.Context) {
	s.inst.UpdatedAt = time.Now().UTC()
	if err := s.store.Save(context.Background(), s.inst); err != nil {
		s.log.Errorf("instance=%s persist failed: %v", s.inst.ID, err)
	}
}

// run executes until every task is terminal or ctx is canceled. The instance
// state is persisted after every transition.
func (s *scheduler) run(ctx context.Context) error {
	s.log.Infof("scheduler start instance=%s run=%s tasks=%d maxParallel=%d",
		s.inst.ID, s.inst.RunID, len(s.inst.Tasks), s.maxParallel)

	s.recoverInterrupted(ctx)

	for {
		if ctx.Err() != nil {
			break
		}

		s.mu.Lock()
		// 1. Propagate terminal dependencies: skip / mark runnable.
		runnable := s.settleLocked()
		nonTerminal := 0
		for _, ts := range s.inst.Tasks {
			if !ts.Status.Terminal() {
				nonTerminal++
			}
		}
		active := s.active
		// 2. Launch within the concurrency budget, stable topo order.
		slots := s.maxParallel - active
		if slots < 0 {
			slots = 0
		}
		if slots > len(runnable) {
			slots = len(runnable)
		}
		for i := 0; i < slots; i++ {
			ts := s.inst.Tasks[runnable[i]]
			ts.Status = TaskRunning
			now := time.Now().UTC()
			ts.StartedAt = &now
			ts.UpdatedAt = now
			ts.Attempts = append(ts.Attempts, Attempt{N: len(ts.Attempts) + 1, StartedAt: now})
			s.persistLocked(ctx)
			s.active++
			s.log.Debugf("dispatch task=%s attempt=%d active=%d", ts.ID, len(ts.Attempts), s.active)
			go s.execute(ctx, ts.ID)
		}
		// Remaining non-terminal work: launched workers + tasks not started
		// this tick (waiting on deps, blocked by concurrency, or retrying).
		remaining := nonTerminal - slots
		activeNow := s.active
		s.mu.Unlock()

		// Wait while workers are in flight, even after cancellation: they
		// own the terminal transition of their task and must finish before
		// finalize touches it. When the run is canceled and no worker can
		// make progress, settleLocked finalizes unreached tasks.
		if activeNow > 0 {
			select {
			case <-s.wake:
			case <-ctx.Done():
			}
			continue
		}
		if remaining == 0 {
			break
		}

		select {
		case <-s.wake:
		case <-ctx.Done():
		}
	}

	return s.finalize(ctx)
}

// settleLocked inspects every pending task whose dependencies are all
// terminal and either skips it (condition false, or bad dependency without
// AllowSkip) or returns it as runnable in topo order. Any state changes are
// persisted.
func (s *scheduler) settleLocked() []string {
	var runnable []string
	changed := false
	for _, id := range s.inst.TopoOrder {
		ts := s.inst.Tasks[id]
		if ts == nil || ts.Status != TaskPending {
			continue
		}
		allTerminal := true
		badDep := ""
		for _, dep := range ts.Spec.DependsOn {
			d := s.inst.Tasks[dep]
			if d == nil || !d.Status.Terminal() {
				allTerminal = false
				break
			}
			if d.Status != TaskSucceeded {
				badDep = dep
			}
		}
		if !allTerminal {
			continue
		}
		if badDep != "" && !ts.Spec.AllowSkip {
			s.markSkippedLocked(ts, fmt.Sprintf("dependency %q did not succeed (status=%s)", badDep, s.inst.Tasks[badDep].Status))
			s.log.Debugf("skip task=%s reason=dep:%s", ts.ID, badDep)
			changed = true
			continue
		}
		if ts.Spec.Condition != nil {
			ok, err := ts.Spec.Condition.Evaluate(s.successOutputsLocked(ts))
			if err != nil {
				// Invalid condition surviving validation is an internal
				// integrity error; fail deterministically.
				s.markTerminalLocked(ts, TaskFailed, "condition error: "+err.Error())
				changed = true
				continue
			}
			if !ok {
				s.markSkippedLocked(ts, "condition evaluated to false")
				s.log.Debugf("skip task=%s reason=condition-false", ts.ID)
				changed = true
				continue
			}
		}
		runnable = append(runnable, id)
	}
	if changed {
		s.persistLocked(context.Background())
	}
	return runnable
}

// successOutputsLocked builds the outputs map visible to a task: outputs of
// its succeeded dependencies only.
func (s *scheduler) successOutputsLocked(ts *TaskState) map[string]any {
	out := make(map[string]any, len(ts.Spec.DependsOn))
	for _, dep := range ts.Spec.DependsOn {
		if d := s.inst.Tasks[dep]; d != nil && d.Status == TaskSucceeded {
			out[dep] = d.Output
		}
	}
	return out
}

func (s *scheduler) markSkippedLocked(ts *TaskState, reason string) {
	ts.Status = TaskSkipped
	ts.Error = reason
	ts.UpdatedAt = time.Now().UTC()
}

func (s *scheduler) markTerminalLocked(ts *TaskState, status TaskStatus, errMsg string) {
	ts.Status = status
	ts.Error = errMsg
	ts.UpdatedAt = time.Now().UTC()
}

// recoverInterrupted closes attempts left "running" by a crashed process and
// either re-queues the task (attempts remain within policy) or fails it.
func (s *scheduler) recoverInterrupted(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	for _, id := range s.inst.TopoOrder {
		ts := s.inst.Tasks[id]
		if ts == nil || ts.Status != TaskRunning {
			continue
		}
		now := time.Now().UTC()
		if len(ts.Attempts) > 0 {
			a := &ts.Attempts[len(ts.Attempts)-1]
			if a.EndedAt == nil {
				a.EndedAt = &now
				a.Outcome = "interrupted"
				a.Error = "process restarted before attempt completed"
			}
		}
		if uint32(len(ts.Attempts)) < effectiveMaxAttempts(ts.Spec.Retry) {
			ts.Status = TaskPending
			ts.StartedAt = nil
			ts.Error = "re-queued after process restart"
			s.log.Infof("recover task=%s requeue (attempts=%d/%d)",
				ts.ID, len(ts.Attempts), ts.Spec.Retry.MaxAttempts)
		} else {
			ts.Status = TaskFailed
			ts.Error = "retries exhausted after process restart"
			s.log.Warnf("recover task=%s fail after restart (attempts=%d)", ts.ID, len(ts.Attempts))
		}
		ts.UpdatedAt = now
		changed = true
	}
	// Tasks marked ready but not running are simply re-queued.
	for _, ts := range s.inst.Tasks {
		if ts.Status == TaskReady {
			ts.Status = TaskPending
			changed = true
		}
	}
	if changed {
		s.persistLocked(ctx)
	}
}

// execute runs the attempt/backoff loop for one task. Exactly one execute
// goroutine runs per dispatch; it finishes by decrementing s.active.
func (s *scheduler) execute(ctx context.Context, id string) {
	defer func() {
		s.mu.Lock()
		s.active--
		s.persistLocked(ctx)
		s.mu.Unlock()
		s.notify()
	}()

	for {
		if ctx.Err() != nil {
			s.cancelTask(ctx, id, "run context canceled before attempt")
			return
		}

		handler, in, timeout, ts := s.prepareAttempt(id)
		attemptCtx := ctx
		var cancel context.CancelFunc
		var timed bool
		if timeout > 0 {
			attemptCtx, cancel = context.WithTimeoutCause(ctx, timeout,
				fmt.Errorf("%w: task %q exceeded %s", ErrTaskTimedOut, id, timeout))
			timed = true
		} else {
			cancelCtx, c := context.WithCancelCause(ctx)
			attemptCtx = cancelCtx
			cancel = func() { c(nil) }
		}
		defer cancel()
		s.log.Debugf("run task=%s attempt=%d timeout=%s", id, in.Attempt, timeout)
		res, err := handler(attemptCtx, in)
		// Do NOT cancel synchronously here: canceling attemptCtx would make
		// context.Cause report "context canceled" while we classify the
		// result below. The deferred cancel releases resources afterwards.

		// classify categorizes the outcome of one handler invocation.
		classify := func(err error) (outcome string, msg string, retryable bool) {
			// Parent run canceled: never retry, never a success.
			if ctx.Err() != nil {
				return "canceled", ctx.Err().Error(), false
			}
			cause := context.Cause(attemptCtx)
			switch {
			case errors.Is(cause, ErrTaskTimedOut):
				return "timeout", cause.Error(), true
			case cause != nil && timed:
				return "timeout", cause.Error(), true
			case errors.Is(err, context.DeadlineExceeded):
				return "timeout", fmt.Sprintf("timeout after %s", timeout), true
			case err == nil:
				return "success", "", false
			case isSpawn(err):
				return "spawn", "", false
			case errors.Is(err, context.Canceled) || errors.Is(cause, context.Canceled):
				return "canceled", "context canceled", false
			default:
				if err != nil {
					return "error", err.Error(), true
				}
				return "success", "", false
			}
		}

		outcome, msg, retryable := classify(err)
		switch outcome {
		case "success":
			if s.completeSuccess(ctx, ts, res, nil) {
				return
			}
		case "spawn":
			var sp *SpawnTasksError
			errors.As(err, &sp)
			if s.completeSuccess(ctx, ts, sp.Result, sp) {
				return
			}
		case "canceled":
			s.finishAttempt(ctx, ts, "canceled", msg, false)
			s.transition(ctx, ts, TaskCanceled, msg)
			s.log.Infof("task=%s canceled", id)
			return
		case "timeout":
			s.finishAttempt(ctx, ts, "timeout", msg, true)
			if s.maybeRetry(ctx, ts) {
				continue
			}
			s.transition(ctx, ts, TaskTimedOut, msg+"; retries exhausted")
			s.log.Warnf("task=%s timed-out: %s", id, msg)
			return
		default: // error
			s.finishAttempt(ctx, ts, "error", msg, retryable)
			if s.maybeRetry(ctx, ts) {
				continue
			}
			s.transition(ctx, ts, TaskFailed, msg)
			s.log.Warnf("task=%s failed: %s", id, msg)
			return
		}
	}
}

// prepareAttempt snapshots everything a handler invocation needs.
func (s *scheduler) prepareAttempt(id string) (TaskHandler, TaskInput, time.Duration, *TaskState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts := s.inst.Tasks[id]
	h, err := s.reg.Get(ts.Spec.Handler)
	if err != nil {
		// Unknown handler surviving validation: fail on first attempt path.
		h = func(context.Context, TaskInput) (TaskResult, error) {
			return TaskResult{}, err
		}
	}
	attempt := len(ts.Attempts)
	in := TaskInput{
		InstanceID: s.inst.ID,
		TaskID:     id,
		Attempt:    attempt,
		Params:     ts.Spec.Params,
		Outputs:    s.successOutputsLocked(ts),
	}
	timeout := ts.Spec.Timeout
	if timeout == 0 {
		timeout = s.defaultTimeout
	}
	return h, in, timeout, ts
}

// finishAttempt closes the current attempt record. retryable records the
// planned backoff when the attempt will be retried.
func (s *scheduler) finishAttempt(ctx context.Context, ts *TaskState, outcome, msg string, mayRetry bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if len(ts.Attempts) > 0 {
		a := &ts.Attempts[len(ts.Attempts)-1]
		a.EndedAt = &now
		a.Outcome = outcome
		a.Error = msg
		if mayRetry {
			a.Backoff = backoff(ts.Spec.Retry, len(ts.Attempts))
		}
	}
	ts.UpdatedAt = now
	s.persistLocked(ctx)
}

// maybeRetry either sleeps the backoff and appends a fresh attempt record,
// or returns false when attempts are exhausted / run canceled.
func (s *scheduler) maybeRetry(ctx context.Context, ts *TaskState) bool {
	max := effectiveMaxAttempts(ts.Spec.Retry)
	done := uint32(len(ts.Attempts))
	if done >= max {
		return false
	}
	d := backoff(ts.Spec.Retry, int(done))
	s.log.Infof("task=%s retry %d/%d in %s", ts.ID, done+1, max, d)
	if d > 0 {
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return false
		}
	}
	if ctx.Err() != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	ts.Status = TaskRunning
	ts.UpdatedAt = now
	ts.Attempts = append(ts.Attempts, Attempt{N: len(ts.Attempts) + 1, StartedAt: now})
	s.persistLocked(ctx)
	return true
}

// transition records a terminal status and persists.
func (s *scheduler) transition(ctx context.Context, ts *TaskState, status TaskStatus, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts.Status = status
	ts.Error = msg
	ts.UpdatedAt = time.Now().UTC()
	s.persistLocked(ctx)
	s.log.Infof("task=%s -> %s", ts.ID, status)
}

func (s *scheduler) cancelTask(ctx context.Context, id, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts := s.inst.Tasks[id]
	if ts.Status.Terminal() {
		return
	}
	now := time.Now().UTC()
	if len(ts.Attempts) > 0 {
		a := &ts.Attempts[len(ts.Attempts)-1]
		if a.EndedAt == nil {
			a.EndedAt = &now
			a.Outcome = "canceled"
			a.Error = msg
		}
	}
	ts.Status = TaskCanceled
	ts.Error = msg
	ts.UpdatedAt = now
	s.persistLocked(ctx)
	s.log.Infof("task=%s -> canceled", id)
}

// completeSuccess handles the nil-error and spawn paths. Returns true when
// the task is terminally done. On spawn validation failure it records the
// attempt as an error and (maybe) retries, returning false.
func (s *scheduler) completeSuccess(ctx context.Context, ts *TaskState, res TaskResult, sp *SpawnTasksError) bool {
	if sp != nil {
		if err := s.applySpawn(ctx, ts, sp); err != nil {
			s.finishAttempt(ctx, ts, "error", "rejected spawn: "+err.Error(), true)
			if s.maybeRetry(ctx, ts) {
				return false
			}
			s.transition(ctx, ts, TaskFailed, "rejected spawn: "+err.Error())
			return true
		}
	}
	s.mu.Lock()
	now := time.Now().UTC()
	if len(ts.Attempts) > 0 {
		a := &ts.Attempts[len(ts.Attempts)-1]
		a.EndedAt = &now
		a.Outcome = "success"
	}
	ts.Status = TaskSucceeded
	ts.Output = res.Output
	ts.Error = ""
	ts.UpdatedAt = now
	s.persistLocked(ctx)
	s.mu.Unlock()
	s.log.Infof("task=%s -> succeeded", ts.ID)
	return true
}

// applySpawn validates and appends dynamically generated tasks. It is atomic:
// on any validation error nothing is added.
func (s *scheduler) applySpawn(ctx context.Context, spawner *TaskState, sp *SpawnTasksError) error {
	var newTasks []TaskSpec
	for _, req := range sp.Requests {
		newTasks = append(newTasks, req.Tasks...)
	}
	if len(newTasks) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	combined := WorkflowSpec{Name: s.inst.Spec.Name, MaxParallel: s.inst.Spec.MaxParallel}
	seen := map[string]bool{}
	for _, id := range s.inst.TopoOrder {
		combined.Tasks = append(combined.Tasks, s.inst.Tasks[id].Spec)
		seen[id] = true
	}
	for _, t := range newTasks {
		if seen[t.ID] {
			return fmt.Errorf("%w: dynamic task id %q already exists", ErrDuplicateTaskID, t.ID)
		}
		combined.Tasks = append(combined.Tasks, t)
		seen[t.ID] = true
	}
	if err := Validate(combined, s.reg); err != nil {
		return err
	}

	now := time.Now().UTC()
	pos := indexOf(s.inst.TopoOrder, spawner.ID) + 1
	for i := range newTasks {
		t := newTasks[i]
		if _, exists := s.inst.Tasks[t.ID]; exists {
			return fmt.Errorf("%w: dynamic task id %q already exists", ErrDuplicateTaskID, t.ID)
		}
		s.inst.Tasks[t.ID] = &TaskState{ID: t.ID, Spec: t, Status: TaskPending, UpdatedAt: now}
		s.inst.TopoOrder = insertAt(s.inst.TopoOrder, pos, t.ID)
		pos++
		s.log.Infof("spawn task=%s by=%s", t.ID, spawner.ID)
	}
	s.persistLocked(ctx)
	return nil
}

// finalize marks leftover non-terminal tasks after cancellation and computes
// the terminal instance status.
func (s *scheduler) finalize(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	canceled := ctx.Err() != nil
	var failed, timedOut, canceledTasks []string
	for _, id := range s.inst.TopoOrder {
		ts := s.inst.Tasks[id]
		if !ts.Status.Terminal() {
			if canceled {
				ts.Status = TaskCanceled
				if ts.Error == "" {
					ts.Error = "run context canceled"
				}
			} else {
				// Defensive: should not happen in normal scheduling.
				ts.Status = TaskFailed
				ts.Error = "scheduler stopped with task unfinished"
			}
			ts.UpdatedAt = time.Now().UTC()
		}
		switch ts.Status {
		case TaskFailed:
			failed = append(failed, id)
		case TaskTimedOut:
			timedOut = append(timedOut, id)
		case TaskCanceled:
			canceledTasks = append(canceledTasks, id)
		}
	}

	switch {
	case len(failed) > 0 || len(timedOut) > 0:
		s.inst.Status = InstanceFailed
		s.inst.Error = summarizeFailures(s.inst.Tasks, failed, timedOut)
	case canceled:
		s.inst.Status = InstanceCanceled
		s.inst.Error = "canceled"
	default:
		s.inst.Status = InstanceSucceeded
		s.inst.Error = ""
	}
	s.persistLocked(ctx)
	s.log.Infof("scheduler done instance=%s status=%s failed=%v timedOut=%v canceled=%v",
		s.inst.ID, s.inst.Status, failed, timedOut, canceledTasks)

	switch s.inst.Status {
	case InstanceFailed:
		if len(timedOut) > 0 {
			return fmt.Errorf("%w: instance=%s failed=%v timedOut=%v", ErrTaskTimedOut, s.inst.ID, failed, timedOut)
		}
		return fmt.Errorf("%w: instance=%s failed=%v", ErrTaskFailed, s.inst.ID, failed)
	case InstanceCanceled:
		return fmt.Errorf("%w: instance=%s tasks=%v", ErrTaskCanceled, s.inst.ID, canceledTasks)
	default:
		return nil
	}
}

func summarizeFailures(tasks map[string]*TaskState, failed, timedOut []string) string {
	var b strings.Builder
	for _, id := range failed {
		fmt.Fprintf(&b, "%s failed: %s; ", id, tasks[id].Error)
	}
	for _, id := range timedOut {
		fmt.Fprintf(&b, "%s timed_out: %s; ", id, tasks[id].Error)
	}
	return strings.TrimRight(b.String(), "; ")
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func effectiveMaxAttempts(p RetryPolicy) uint32 {
	if p.MaxAttempts <= 0 {
		return 1
	}
	return uint32(p.MaxAttempts)
}

// backoff returns the delay BEFORE the (done+1)-th attempt, i.e. after `done`
// attempts. done is 1-based count already executed.
func backoff(p RetryPolicy, done int) time.Duration {
	if p.Backoff <= 0 || done <= 0 {
		return 0
	}
	factor := p.Factor
	if factor < 1 {
		factor = 1
	}
	d := float64(p.Backoff)
	for i := 1; i < done; i++ {
		d *= factor
	}
	if p.MaxBackoff > 0 && d > float64(p.MaxBackoff) {
		d = float64(p.MaxBackoff)
	}
	return time.Duration(d)
}

func isSpawn(err error) bool {
	var sp *SpawnTasksError
	return errors.As(err, &sp)
}

func indexOf(xs []string, v string) int {
	for i, x := range xs {
		if x == v {
			return i
		}
	}
	return -1
}

// insertAt inserts id into xs at position at (clamped), returning a new slice.
func insertAt(xs []string, at int, id string) []string {
	if at < 0 {
		at = 0
	}
	if at > len(xs) {
		at = len(xs)
	}
	out := make([]string, 0, len(xs)+1)
	out = append(out, xs[:at]...)
	out = append(out, id)
	out = append(out, xs[at:]...)
	return out
}
