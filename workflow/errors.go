package workflow

import "fmt"

// DefinitionErrorCode 标识工作流定义被拒绝的具体原因，便于调用方区分处理。
type DefinitionErrorCode string

const (
	// ErrCodeDuplicateID 重复任务标识。
	ErrCodeDuplicateID DefinitionErrorCode = "duplicate_task_id"
	// ErrCodeMissingDependency 依赖了不存在的任务。
	ErrCodeMissingDependency DefinitionErrorCode = "missing_dependency"
	// ErrCodeCyclicDependency 任务依赖图存在环。
	ErrCodeCyclicDependency DefinitionErrorCode = "cyclic_dependency"
	// ErrCodeInvalidConfig 任务/工作流配置无效。
	ErrCodeInvalidConfig DefinitionErrorCode = "invalid_config"
)

// DefinitionError 表示工作流定义非法。Code 可用于区分拒绝原因。
type DefinitionError struct {
	Code    DefinitionErrorCode
	Message string
	TaskID  string // 可选：出错的任务标识
}

func (e *DefinitionError) Error() string {
	if e.TaskID != "" {
		return fmt.Sprintf("workflow definition error (%s): task=%s: %s", e.Code, e.TaskID, e.Message)
	}
	return fmt.Sprintf("workflow definition error (%s): %s", e.Code, e.Message)
}

// ExecErrorCode 标识运行期错误原因。
type ExecErrorCode string

const (
	// ErrCodeInstanceRunning 同一实例已在运行，拒绝重复触发。
	ErrCodeInstanceRunning ExecErrorCode = "instance_already_running"
	// ErrCodeInstanceTerminal 实例已处于终态，不可再次触发。
	ErrCodeInstanceTerminal ExecErrorCode = "instance_terminal"
	// ErrCodeTimeout 任务执行超时。
	ErrCodeTimeout ExecErrorCode = "task_timeout"
	// ErrCodeCanceled 工作流/任务被取消。
	ErrCodeCanceled ExecErrorCode = "canceled"
	// ErrCodeTaskFailed 任务重试耗尽后仍失败。
	ErrCodeTaskFailed ExecErrorCode = "task_failed"
	// ErrCodeStateCorrupt 持久化状态损坏或无法解析。
	ErrCodeStateCorrupt ExecErrorCode = "state_corrupt"
)

// ExecError 表示运行期错误。
type ExecError struct {
	Code   ExecErrorCode
	Detail string
}

func (e *ExecError) Error() string {
	return fmt.Sprintf("workflow exec error (%s): %s", e.Code, e.Detail)
}
