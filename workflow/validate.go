package workflow

import (
	"fmt"
	"strings"
)

// Validate 校验工作流定义，拒绝以下非法情形（通过 DefinitionError.Code 区分）：
//   - invalid_config: 定义为空、任务 ID/Handler 为空、重试或超时参数非法、条件表达式非法、
//     同一任务的依赖列表内重复。
//   - duplicate_task_id: 多个任务使用相同 ID。
//   - missing_dependency: DependsOn 或条件表达式引用了不存在的任务。
//   - cyclic_dependency: 依赖图含环（含自依赖）。
func Validate(def *Definition) error {
	if def == nil {
		return &DefinitionError{Code: ErrCodeInvalidConfig, Message: "definition is nil"}
	}
	if strings.TrimSpace(def.Name) == "" {
		return &DefinitionError{Code: ErrCodeInvalidConfig, Message: "workflow name is empty"}
	}
	if len(def.Tasks) == 0 {
		return &DefinitionError{Code: ErrCodeInvalidConfig, Message: "workflow has no tasks"}
	}
	if err := validateRetry(&def.DefaultRetry, ""); err != nil {
		return err
	}

	ids := make(map[string]struct{}, len(def.Tasks))
	for i := range def.Tasks {
		t := &def.Tasks[i]
		if strings.TrimSpace(t.ID) == "" {
			return &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: t.ID, Message: "task id is empty"}
		}
		if strings.TrimSpace(t.Handler) == "" {
			return &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: t.ID, Message: "task handler is empty"}
		}
		if t.Timeout < 0 {
			return &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: t.ID, Message: "task timeout must be >= 0"}
		}
		if t.Retry != nil {
			if err := validateRetry(t.Retry, t.ID); err != nil {
				return err
			}
		}
		if _, dup := ids[t.ID]; dup {
			return &DefinitionError{Code: ErrCodeDuplicateID, TaskID: t.ID, Message: "duplicate task id"}
		}
		ids[t.ID] = struct{}{}

		seen := make(map[string]struct{}, len(t.DependsOn))
		for _, d := range t.DependsOn {
			if strings.TrimSpace(d) == "" {
				return &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: t.ID, Message: "dependency id is empty"}
			}
			if d == t.ID {
				return &DefinitionError{Code: ErrCodeCyclicDependency, TaskID: t.ID, Message: "task depends on itself"}
			}
			if _, dup := seen[d]; dup {
				return &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: t.ID, Message: fmt.Sprintf("duplicate dependency %q", d)}
			}
			seen[d] = struct{}{}
		}
		if t.Condition != "" {
			if err := validateCondition(t.Condition, t.ID); err != nil {
				return err
			}
		}
	}

	// 引用完整性：DependsOn 与条件中的任务引用必须存在。
	for _, t := range def.Tasks {
		for _, d := range t.DependsOn {
			if _, ok := ids[d]; !ok {
				return &DefinitionError{Code: ErrCodeMissingDependency, TaskID: t.ID, Message: fmt.Sprintf("dependency %q does not exist", d)}
			}
		}
		if t.Condition != "" {
			if ref := conditionTaskRef(t.Condition); ref != "" {
				if _, ok := ids[ref]; !ok {
					return &DefinitionError{Code: ErrCodeMissingDependency, TaskID: t.ID, Message: fmt.Sprintf("condition references task %q which does not exist", ref)}
				}
			}
		}
	}

	if err := detectCycle(def); err != nil {
		return err
	}
	return nil
}

func validateRetry(r *RetryPolicy, taskID string) error {
	if r == nil {
		return nil
	}
	if r.MaxAttempts < 0 {
		return &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: taskID, Message: "retry max_attempts must be >= 0"}
	}
	if r.InitialBackoff < 0 || r.MaxBackoff < 0 {
		return &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: taskID, Message: "retry backoff must be >= 0"}
	}
	if r.MaxBackoff > 0 && r.InitialBackoff > r.MaxBackoff {
		return &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: taskID, Message: "retry initial_backoff must be <= max_backoff"}
	}
	return nil
}

// validateCondition 校验条件表达式语法：
//
//	""                        无条件（默认：所有依赖成功才执行）
//	"taskID:status"           status ∈ succeeded|failed|skipped|canceled
//	"cond:registeredName"     具名自定义条件（注册在引擎层校验存在性）
func validateCondition(expr string, taskID string) error {
	parts := strings.Split(expr, ":")
	switch {
	case strings.HasPrefix(expr, "cond:"):
		if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
			return &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: taskID, Message: fmt.Sprintf("invalid condition %q", expr)}
		}
	default:
		if len(parts) != 2 {
			return &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: taskID, Message: fmt.Sprintf("invalid condition %q", expr)}
		}
		st := TaskStatus(parts[1])
		switch st {
		case StatusSucceeded, StatusFailed, StatusSkipped, StatusCanceled:
		default:
			return &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: taskID, Message: fmt.Sprintf("condition %q references unknown status", expr)}
		}
		if strings.TrimSpace(parts[0]) == "" {
			return &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: taskID, Message: fmt.Sprintf("invalid condition %q", expr)}
		}
	}
	return nil
}

// conditionTaskRef 返回条件表达式引用的任务 ID；具名条件返回空串。
func conditionTaskRef(expr string) string {
	if strings.HasPrefix(expr, "cond:") {
		return ""
	}
	idx := strings.IndexByte(expr, ':')
	if idx <= 0 {
		return ""
	}
	return expr[:idx]
}

// detectCycle 用 Kahn 拓扑排序检测环；无法被排序的节点即处于环上。
func detectCycle(def *Definition) error {
	indeg := make(map[string]int, len(def.Tasks))
	dependents := make(map[string][]string, len(def.Tasks))
	for _, t := range def.Tasks {
		indeg[t.ID] += 0
		for _, d := range t.DependsOn {
			indeg[t.ID]++
			dependents[d] = append(dependents[d], t.ID)
		}
	}
	queue := make([]string, 0, len(def.Tasks))
	for id, n := range indeg {
		if n == 0 {
			queue = append(queue, id)
		}
	}
	visited := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		visited++
		for _, next := range dependents[cur] {
			indeg[next]--
			if indeg[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if visited != len(def.Tasks) {
		for _, t := range def.Tasks {
			if indeg[t.ID] > 0 {
				return &DefinitionError{Code: ErrCodeCyclicDependency, TaskID: t.ID, Message: "task participates in a dependency cycle"}
			}
		}
		return &DefinitionError{Code: ErrCodeCyclicDependency, Message: "dependency cycle detected"}
	}
	return nil
}
