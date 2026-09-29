package workflow

import (
	"context"
	"time"
)

// RetryPolicy 描述任务失败后的重试策略。
type RetryPolicy struct {
	// MaxAttempts 最大尝试次数（含首次执行）。<=1 表示不重试。
	MaxAttempts int
	// InitialBackoff 首次重试前的基础退避时间。
	InitialBackoff time.Duration
	// MaxBackoff 退避上限。
	MaxBackoff time.Duration
	// Multiplier 退避倍率（<1 时按 1 处理）。
	Multiplier float64
}

// TaskDefinition 工作流中的一个任务节点定义。
type TaskDefinition struct {
	// ID 任务唯一标识。
	ID string
	// Handler 已注册的处理器名称（可持久化、可恢复）。
	Handler string
	// Params 传给处理器的静态参数。
	Params map[string]any
	// DependsOn 前置依赖任务 ID 列表。
	DependsOn []string
	// Condition 非空时，仅当其求值为 true 才执行；否则标记 Skipped。
	// 形如 "taskID:succeeded" 或注册的条件名 "cond:xxx"。
	Condition string
	// Retry 任务级重试策略；为 nil 时使用工作流默认策略。
	Retry *RetryPolicy
	// Timeout 任务级超时；0 表示使用引擎默认值。
	Timeout time.Duration
	// Dynamic 为 true 表示该任务由运行期动态生成（非静态定义）。
	Dynamic bool
	// FailureBranch 为 true 时，该任务失败不触发整个工作流中止，
	// 而是进入失败终态，由下游的条件表达式（如 "this:failed"）决定分支。
	// 没有条件的默认下游仍按"依赖未成功→跳过"处理。
	FailureBranch bool
}

// Definition 一个完整的工作流定义。
type Definition struct {
	Name         string
	Tasks        []TaskDefinition
	DefaultRetry RetryPolicy
}

// TaskStatus 任务生命周期状态。
type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusReady     TaskStatus = "ready"
	StatusRunning   TaskStatus = "running"
	StatusSucceeded TaskStatus = "succeeded"
	StatusFailed    TaskStatus = "failed"
	StatusSkipped   TaskStatus = "skipped"
	StatusCanceled  TaskStatus = "canceled"
)

// Terminal 报告任务是否已处于终态。
func (s TaskStatus) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusSkipped, StatusCanceled:
		return true
	}
	return false
}

// WorkflowStatus 工作流实例状态。
type WorkflowStatus string

const (
	WFPending   WorkflowStatus = "pending"
	WFRunning   WorkflowStatus = "running"
	WFSucceeded WorkflowStatus = "succeeded"
	WFFailed    WorkflowStatus = "failed"
	WFCanceled  WorkflowStatus = "canceled"
)

// RetryRecord 单次尝试的可追溯记录。
type RetryRecord struct {
	Attempt   int       `json:"attempt"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	Err       string    `json:"err,omitempty"`
}

// TaskState 单个任务实例的持久化状态。
type TaskState struct {
	ID         string         `json:"id"`
	Definition TaskDefinition `json:"definition"`
	Status     TaskStatus     `json:"status"`
	Attempt    int            `json:"attempt"`
	Result     map[string]any `json:"result,omitempty"`
	Err        string         `json:"err,omitempty"`
	Records    []RetryRecord  `json:"records,omitempty"`
	StartedAt  *time.Time     `json:"started_at,omitempty"`
	EndedAt    *time.Time     `json:"ended_at,omitempty"`
}

// State 工作流实例的完整持久化状态。
type State struct {
	WorkflowID     string                `json:"workflow_id"`
	DefinitionName string                `json:"definition_name"`
	Status         WorkflowStatus        `json:"status"`
	Tasks          map[string]*TaskState `json:"tasks"`
	// TaskOrder 保存任务加入顺序，保证可解释、稳定的遍历顺序。
	TaskOrder    []string  `json:"task_order"`
	FailureCause string    `json:"failure_cause,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// ExecContext 传给任务处理器的运行期上下文。
type ExecContext struct {
	Ctx        context.Context
	WorkflowID string
	TaskID     string
	Params     map[string]any
	// Results 前置依赖任务的结果快照（只读语义）。
	Results map[string]map[string]any
	// Spawn 允许任务在运行中动态生成后续任务。
	Spawn func(tasks ...TaskDefinition) error
	// Logger 供处理器打印关键输入/决策。
	Logger Logger
}

// TaskHandler 任务处理函数。返回 error 视为失败（可按策略重试）。
type TaskHandler func(ec *ExecContext) (map[string]any, error)

// ConditionFunc 自定义条件求值。输入为全部任务状态，返回是否应执行。
type ConditionFunc func(results map[string]*TaskState) bool

// Config 引擎配置。
type Config struct {
	// StateDir JSON 状态文件目录（持久化）。
	StateDir string
	// MaxConcurrency 同一实例内并发执行任务数上限；<=0 表示不限。
	MaxConcurrency int
	// DefaultTimeout 任务默认超时；0 表示不超时。
	DefaultTimeout time.Duration
}
