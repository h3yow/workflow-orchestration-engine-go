package workflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// Engine is the entry point for defining, triggering and resuming workflows.
type Engine struct {
	cfg   Config
	store Store
	reg   *Registry
	log   Logger
}

// NewEngine constructs an engine with a FileStore rooted at cfg.StateDir and
// an empty handler registry.
func NewEngine(cfg Config) (*Engine, error) {
	store, err := NewFileStore(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	return NewEngineWithStore(cfg, store, NewRegistry()), nil
}

// NewEngineWithStore constructs an engine with injected dependencies
// (in-memory store / custom registry), used primarily in tests.
func NewEngineWithStore(cfg Config, store Store, reg *Registry) *Engine {
	if reg == nil {
		reg = NewRegistry()
	}
	return &Engine{cfg: cfg, store: store, reg: reg, log: newStdLogger()}
}

// Registry exposes the handler registry so callers can register handlers.
func (e *Engine) Registry() *Registry { return e.reg }

// SetLogger replaces the default stderr logger (e.g. with a test logger).
func (e *Engine) SetLogger(l Logger) {
	if l != nil {
		e.log = l
	}
}

// Define validates a spec. The returned spec is normalized (e.g. nil retry
// fields default) but otherwise identical.
func (e *Engine) Define(spec WorkflowSpec) (*WorkflowSpec, error) {
	if err := Validate(spec, e.reg); err != nil {
		return nil, err
	}
	return &spec, nil
}

// Start creates a new instance and runs it synchronously to a terminal state.
//
// Duplicate-trigger semantics:
//   - the same instance ID while a run is active (this process or another
//     holding the lock) => ErrInstanceActive immediately;
//   - the same instance ID after a previous terminal run => ErrInstanceTerminal;
//   - the same instance ID left non-terminal by a crash => the new run acts as
//     recovery (equivalent to Resume): completed tasks are not re-executed.
//
// A run that finishes failed/canceled/timed-out returns the terminal instance
// together with an error wrapping ErrTaskFailed / ErrTaskTimedOut /
// ErrTaskCanceled.
func (e *Engine) Start(ctx context.Context, spec WorkflowSpec, instanceID string) (*Instance, error) {
	logInput := e.log
	logInput.Infof("Start request instance=%s spec=%s tasks=%d", instanceID, spec.Name, len(spec.Tasks))

	if _, err := e.Define(spec); err != nil {
		return nil, err
	}

	unlock, err := e.store.TryLock(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	defer unlock()

	inst, loadErr := e.store.Load(ctx, instanceID)
	switch {
	case loadErr == nil:
		if inst.Status.Terminal() {
			e.log.Warnf("Start rejected instance=%s already terminal status=%s", instanceID, inst.Status)
			return inst, fmt.Errorf("%w: instance %q status=%s", ErrInstanceTerminal, instanceID, inst.Status)
		}
		e.log.Infof("Start instance=%s found unfinished state; resuming in place", instanceID)
		return e.runLocked(ctx, inst)
	case !errors.Is(loadErr, ErrInstanceNotFound):
		return nil, loadErr
	}

	inst = newInstance(spec, instanceID)
	inst.Status = InstanceRunning
	if err := e.store.Create(ctx, inst); err != nil {
		return nil, err
	}
	e.log.Infof("created instance=%s run=%s", inst.ID, inst.RunID)
	return e.runLocked(ctx, inst)
}

// Resume continues a persisted unfinished instance. Completed tasks are never
// re-executed; tasks interrupted mid-attempt are retried according to policy.
// Resuming a terminal instance returns ErrInstanceTerminal.
func (e *Engine) Resume(ctx context.Context, instanceID string) (*Instance, error) {
	unlock, err := e.store.TryLock(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	defer unlock()

	inst, err := e.store.Load(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if inst.Status.Terminal() {
		return inst, fmt.Errorf("%w: instance %q status=%s", ErrInstanceTerminal, instanceID, inst.Status)
	}
	e.log.Infof("Resume instance=%s previousStatus=%s run=%s", instanceID, inst.Status, inst.RunID)
	return e.runLocked(ctx, inst)
}

// runLocked executes one scheduler run under the held instance lock.
func (e *Engine) runLocked(ctx context.Context, inst *Instance) (*Instance, error) {
	inst.RunID = newRunID()
	if inst.Status == InstancePending {
		inst.Status = InstanceRunning
	}
	maxParallel := inst.Spec.MaxParallel
	if maxParallel <= 0 {
		maxParallel = e.cfg.MaxParallel
	}
	if maxParallel <= 0 {
		maxParallel = 1
	}
	if err := e.store.Save(ctx, inst); err != nil {
		return nil, err
	}

	sc := newScheduler(inst, e.store, e.reg, e.log, maxParallel, e.cfg.TaskTimeout)
	runErr := sc.run(ctx)

	// Reload the authoritative final state (same pointer, but Load keeps
	// Status semantics honest for callers relying on a fresh read).
	final, err := e.store.Load(ctx, inst.ID)
	if err != nil {
		return inst, runErr
	}
	e.log.Infof("final state instance=%s status=%s", final.ID, final.Status)
	return final, runErr
}

// Status returns a snapshot of an instance without taking its run lock.
func (e *Engine) Status(ctx context.Context, instanceID string) (*Instance, error) {
	return e.store.Load(ctx, instanceID)
}

// ListInstances returns ids of all persisted instances.
func (e *Engine) ListInstances(ctx context.Context) ([]string, error) {
	return e.store.List(ctx)
}

// newInstance builds initial runtime state from a validated spec.
func newInstance(spec WorkflowSpec, id string) *Instance {
	now := time.Now().UTC()
	order := topoSort(spec)
	tasks := make(map[string]*TaskState, len(spec.Tasks))
	for _, t := range spec.Tasks {
		tasks[t.ID] = &TaskState{ID: t.ID, Spec: t, Status: TaskPending, UpdatedAt: now}
	}
	return &Instance{
		ID:        id,
		SpecName:  spec.Name,
		Spec:      spec,
		Status:    InstancePending,
		Tasks:     tasks,
		TopoOrder: order,
		CreatedAt: now,
		UpdatedAt: now,
		RunID:     newRunID(),
	}
}

func newRunID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("run-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
