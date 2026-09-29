package workflow

import "errors"

// Distinguishable error causes. Callers can use errors.Is on these sentinels,
// or inspect *ValidationError for definition problems.
var (
	// Definition / validation errors.
	ErrDuplicateTaskID   = errors.New("workflow: duplicate task id")
	ErrMissingDependency = errors.New("workflow: task depends on unknown task id")
	ErrSelfDependency    = errors.New("workflow: task depends on itself")
	ErrCyclicDependency  = errors.New("workflow: cyclic dependency detected")
	ErrInvalidConfig     = errors.New("workflow: invalid configuration")
	ErrUnknownHandler    = errors.New("workflow: unknown handler name")

	// Trigger / instance errors.
	ErrInstanceNotFound = errors.New("workflow: instance not found")
	ErrInstanceActive   = errors.New("workflow: instance already active")
	ErrInstanceTerminal = errors.New("workflow: instance already terminal")
	ErrInvalidState     = errors.New("workflow: corrupt or incompatible persisted state")

	// Runtime failure categories.
	ErrTaskFailed   = errors.New("workflow: task failed")
	ErrTaskTimedOut = errors.New("workflow: task timed out")
	ErrTaskCanceled = errors.New("workflow: task canceled")
)

// ValidationError carries the offending task ID(s) plus the specific cause.
// Callers can distinguish rejection reasons with errors.Is(err, ErrCyclic...),
// errors.As into *ValidationError for detail, or the Kind method.
type ValidationError struct {
	Cause   error    // one of ErrDuplicateTaskID / ErrMissingDependency / ...
	TaskID  string   // task whose definition is problematic (may be empty)
	Detail  string   // human-readable detail
	Related []string // related task ids, e.g. cycle members
}

func (e *ValidationError) Error() string {
	msg := e.Cause.Error()
	if e.TaskID != "" {
		msg += " (task=" + e.TaskID + ")"
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

func (e *ValidationError) Unwrap() error { return e.Cause }
