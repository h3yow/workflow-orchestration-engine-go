package workflow

import "fmt"

// Validate checks a workflow definition for:
//   - empty workflow name / empty task list (ErrInvalidConfig)
//   - empty task id or handler, negative timeout, bad retry/condition config
//   - duplicate task IDs (ErrDuplicateTaskID)
//   - self / unknown / duplicated dependencies (ErrSelfDependency,
//     ErrMissingDependency)
//   - unknown handlers (ErrUnknownHandler)
//   - dependency cycles (ErrCyclicDependency)
//
// Every returned error wraps one of the distinguishable Err* sentinels and is
// typically a *ValidationError carrying the offending task id.
func Validate(spec WorkflowSpec, reg *Registry) error {
	if spec.Name == "" {
		return &ValidationError{Cause: ErrInvalidConfig, Detail: "workflow name is empty"}
	}
	if len(spec.Tasks) == 0 {
		return &ValidationError{Cause: ErrInvalidConfig, Detail: "workflow has no tasks"}
	}
	if spec.MaxParallel < 0 {
		return &ValidationError{Cause: ErrInvalidConfig, Detail: "max_parallel must be >= 0"}
	}

	ids := make(map[string]bool, len(spec.Tasks))
	for i := range spec.Tasks {
		t := &spec.Tasks[i]
		if t.ID == "" {
			return &ValidationError{Cause: ErrInvalidConfig, Detail: fmt.Sprintf("task at index %d has empty id", i)}
		}
		if t.Handler == "" {
			return &ValidationError{Cause: ErrInvalidConfig, TaskID: t.ID, Detail: "handler is empty"}
		}
		if t.Timeout < 0 {
			return &ValidationError{Cause: ErrInvalidConfig, TaskID: t.ID, Detail: "timeout must be >= 0"}
		}
		if t.Retry.MaxAttempts < 0 {
			return &ValidationError{Cause: ErrInvalidConfig, TaskID: t.ID, Detail: "retry.max_attempts must be >= 0"}
		}
		if t.Retry.Backoff < 0 || t.Retry.MaxBackoff < 0 {
			return &ValidationError{Cause: ErrInvalidConfig, TaskID: t.ID, Detail: "retry backoff must be >= 0"}
		}
		if t.Retry.Factor < 0 {
			return &ValidationError{Cause: ErrInvalidConfig, TaskID: t.ID, Detail: "retry factor must be >= 0"}
		}
		if t.Condition != nil {
			if err := t.Condition.validate(); err != nil {
				return &ValidationError{Cause: err, TaskID: t.ID}
			}
		}
		if ids[t.ID] {
			return &ValidationError{Cause: ErrDuplicateTaskID, TaskID: t.ID,
				Detail: "task id appears more than once"}
		}
		ids[t.ID] = true
	}

	// Dependency references.
	seenDep := make(map[string]map[string]bool, len(spec.Tasks))
	for i := range spec.Tasks {
		t := &spec.Tasks[i]
		seenDep[t.ID] = make(map[string]bool)
		for _, dep := range t.DependsOn {
			if dep == t.ID {
				return &ValidationError{Cause: ErrSelfDependency, TaskID: t.ID,
					Detail: "task lists itself as dependency"}
			}
			if !ids[dep] {
				return &ValidationError{Cause: ErrMissingDependency, TaskID: t.ID,
					Detail: fmt.Sprintf("unknown dependency %q", dep), Related: []string{dep}}
			}
			if seenDep[t.ID][dep] {
				return &ValidationError{Cause: ErrInvalidConfig, TaskID: t.ID,
					Detail: fmt.Sprintf("dependency %q listed twice", dep), Related: []string{dep}}
			}
			seenDep[t.ID][dep] = true
		}
	}

	if reg != nil {
		for i := range spec.Tasks {
			t := &spec.Tasks[i]
			if _, err := reg.Get(t.Handler); err != nil {
				return &ValidationError{Cause: ErrUnknownHandler, TaskID: t.ID,
					Detail: err.Error()}
			}
		}
	}

	if cycle := findCycle(spec); cycle != nil {
		tid := ""
		if len(cycle) > 0 {
			tid = cycle[0]
		}
		return &ValidationError{Cause: ErrCyclicDependency, TaskID: tid,
			Detail: "cycle: " + joinIDs(cycle), Related: cycle}
	}
	return nil
}

// findCycle performs colored DFS and returns the ids on the first cycle.
func findCycle(spec WorkflowSpec) []string {
	deps := make(map[string][]string, len(spec.Tasks))
	order := make([]string, 0, len(spec.Tasks))
	for _, t := range spec.Tasks {
		deps[t.ID] = t.DependsOn
		order = append(order, t.ID)
	}
	const white, gray, black = 0, 1, 2
	color := make(map[string]int, len(order))
	var stack []string

	var dfs func(string) []string
	dfs = func(id string) []string {
		color[id] = gray
		stack = append(stack, id)
		for _, dep := range deps[id] {
			switch color[dep] {
			case white:
				if cyc := dfs(dep); cyc != nil {
					return cyc
				}
			case gray:
				// Extract the slice of the stack from dep onward.
				for i, n := range stack {
					if n == dep {
						out := append([]string{}, stack[i:]...)
						return append(out, dep)
					}
				}
			}
		}
		color[id] = black
		stack = stack[:len(stack)-1]
		return nil
	}

	for _, id := range order {
		if color[id] == white {
			if cyc := dfs(id); cyc != nil {
				return cyc
			}
		}
	}
	return nil
}

// topoSort returns task ids in a stable dependency-first order: Kahn's
// algorithm with ties broken by definition order. Assumes a validated DAG.
func topoSort(spec WorkflowSpec) []string {
	index := make(map[string]int, len(spec.Tasks))
	indeg := make(map[string]int, len(spec.Tasks))
	dependents := make(map[string][]string)
	for i, t := range spec.Tasks {
		index[t.ID] = i
		indeg[t.ID] = len(t.DependsOn)
		for _, dep := range t.DependsOn {
			dependents[dep] = append(dependents[dep], t.ID)
		}
	}

	// Ready set scanned in definition order for determinism.
	remaining := make(map[string]bool, len(spec.Tasks))
	for _, t := range spec.Tasks {
		remaining[t.ID] = true
	}
	out := make([]string, 0, len(spec.Tasks))
	for len(remaining) > 0 {
		picked := ""
		for _, t := range spec.Tasks {
			if remaining[t.ID] && indeg[t.ID] == 0 {
				picked = t.ID
				break
			}
		}
		if picked == "" { // should be impossible after validation
			return out
		}
		out = append(out, picked)
		delete(remaining, picked)
		for _, dep := range dependents[picked] {
			indeg[dep]--
		}
	}
	return out
}

func joinIDs(ids []string) string {
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += " -> "
		}
		out += id
	}
	return out
}
