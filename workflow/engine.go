package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Engine 本地工作流编排引擎：按依赖调度、条件分支、动态生成任务、
// 并发上限、超时、取消、重试，并把全部状态持久化到本地 Store。
type Engine struct {
	cfg    Config
	store  Store
	logger Logger

	handlers   map[string]TaskHandler
	conditions map[string]ConditionFunc
	regMu      sync.RWMutex

	// runMu 保证同一实例在本进程内不会被并发执行（重复触发返回错误）。
	runMu map[string]*sync.Mutex
	mu    sync.Mutex

	// cancels 保存运行中实例的取消函数，供 Cancel 调用。
	cancels map[string]context.CancelFunc
}

// NewEngine 创建引擎。StateDir 为空时使用内存存储（仅测试用，重启即丢）。
func NewEngine(cfg Config) (*Engine, error) {
	var st Store
	if cfg.StateDir == "" {
		st = newMemStore()
	} else {
		fs, err := NewFileStore(cfg.StateDir)
		if err != nil {
			return nil, err
		}
		st = fs
	}
	logger := NewStdLogger()
	return &Engine{
		cfg:        cfg,
		store:      st,
		logger:     logger,
		handlers:   map[string]TaskHandler{},
		conditions: map[string]ConditionFunc{},
		runMu:      map[string]*sync.Mutex{},
		cancels:    map[string]context.CancelFunc{},
	}, nil
}

// WithLogger 替换引擎日志器（必须在启动工作流前调用）。
func (e *Engine) WithLogger(l Logger) *Engine {
	if l != nil {
		e.logger = l
	}
	return e
}

func (e *Engine) instanceLock(id string) *sync.Mutex {
	e.mu.Lock()
	defer e.mu.Unlock()
	m, ok := e.runMu[id]
	if !ok {
		m = &sync.Mutex{}
		e.runMu[id] = m
	}
	return m
}

// RegisterHandler 注册具名任务处理器。重复注册与空处理器会被拒绝。
func (e *Engine) RegisterHandler(name string, h TaskHandler) error {
	if name == "" {
		return errors.New("handler name is empty")
	}
	if h == nil {
		return fmt.Errorf("handler %q is nil", name)
	}
	e.regMu.Lock()
	defer e.regMu.Unlock()
	if _, exists := e.handlers[name]; exists {
		return fmt.Errorf("handler %q already registered", name)
	}
	e.handlers[name] = h
	return nil
}

// RegisterCondition 注册具名条件函数（供定义里的 "cond:name" 使用）。
func (e *Engine) RegisterCondition(name string, f ConditionFunc) error {
	if name == "" {
		return errors.New("condition name is empty")
	}
	if f == nil {
		return fmt.Errorf("condition %q is nil", name)
	}
	e.regMu.Lock()
	defer e.regMu.Unlock()
	if _, exists := e.conditions[name]; exists {
		return fmt.Errorf("condition %q already registered", name)
	}
	e.conditions[name] = f
	return nil
}

func (e *Engine) lookupHandler(name string) (TaskHandler, bool) {
	e.regMu.RLock()
	defer e.regMu.RUnlock()
	h, ok := e.handlers[name]
	return h, ok
}

func (e *Engine) lookupCondition(name string) (ConditionFunc, bool) {
	e.regMu.RLock()
	defer e.regMu.RUnlock()
	f, ok := e.conditions[name]
	return f, ok
}

// Start 触发一次工作流执行（阻塞到结束）。
//   - 定义非法：*DefinitionError
//   - 同一实例正在运行 / 已在终态：*ExecError
func (e *Engine) Start(ctx context.Context, def *Definition, workflowID string) (*State, error) {
	if err := Validate(def); err != nil {
		return nil, err
	}
	if workflowID == "" {
		return nil, errors.New("workflow id is empty")
	}
	for i := range def.Tasks {
		t := &def.Tasks[i]
		if t.Condition != "" {
			if name := condName(t.Condition); name != "" {
				if _, ok := e.lookupCondition(name); !ok {
					return nil, &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: t.ID, Message: fmt.Sprintf("condition %q is not registered", name)}
				}
			}
		}
		if _, ok := e.lookupHandler(t.Handler); !ok {
			return nil, &DefinitionError{Code: ErrCodeInvalidConfig, TaskID: t.ID, Message: fmt.Sprintf("handler %q is not registered", t.Handler)}
		}
	}

	lock := e.instanceLock(workflowID)
	if !lock.TryLock() {
		return nil, &ExecError{Code: ErrCodeInstanceRunning, Detail: "workflow instance is already running: " + workflowID}
	}
	defer lock.Unlock()

	existing, err := e.store.Load(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		switch existing.Status {
		case WFRunning, WFPending:
			return nil, &ExecError{Code: ErrCodeInstanceRunning, Detail: "workflow instance is active from a previous run; use Resume: " + workflowID}
		case WFSucceeded, WFFailed, WFCanceled:
			return nil, &ExecError{Code: ErrCodeInstanceTerminal, Detail: fmt.Sprintf("workflow instance already %s: %s", existing.Status, workflowID)}
		}
	}

	st := newStateFromDefinition(def, workflowID)
	return e.run(ctx, st)
}

// Resume 恢复一个实例：已处于终态的任务不会重复执行；
// 崩溃前处于 running 的任务视为未完成，重新按重试策略执行。
func (e *Engine) Resume(ctx context.Context, workflowID string) (*State, error) {
	lock := e.instanceLock(workflowID)
	if !lock.TryLock() {
		return nil, &ExecError{Code: ErrCodeInstanceRunning, Detail: "workflow instance is already running: " + workflowID}
	}
	defer lock.Unlock()

	st, err := e.store.Load(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, &ExecError{Code: ErrCodeStateCorrupt, Detail: "no persisted state for workflow: " + workflowID}
	}
	if st.Status == WFSucceeded || st.Status == WFFailed || st.Status == WFCanceled {
		return st, nil // 幂等：终态直接返回，不做任何执行
	}
	// 崩溃恢复：running 任务没有终态记录，重置为 pending 并清零未完成的尝试。
	for _, ts := range st.Tasks {
		if ts.Status == StatusRunning || ts.Status == StatusReady {
			e.logger.Warn(ctx, "recover unfinished task, reset to pending", "task_id", ts.ID, "prev_status", ts.Status)
			ts.Status = StatusPending
			ts.Attempt = 0
			ts.StartedAt = nil
		}
	}
	st.Status = WFRunning
	if err := e.store.Save(ctx, st); err != nil {
		return nil, err
	}
	return e.run(ctx, st)
}

// RecoverAll 扫描状态目录，恢复所有处于非终态的实例（顺序执行，保证可预测）。
func (e *Engine) RecoverAll(ctx context.Context) ([]string, error) {
	ids, err := e.store.List(ctx)
	if err != nil {
		return nil, err
	}
	var recovered []string
	for _, id := range ids {
		st, err := e.store.Load(ctx, id)
		if err != nil {
			return recovered, err
		}
		if st == nil {
			continue
		}
		if st.Status == WFSucceeded || st.Status == WFFailed || st.Status == WFCanceled {
			continue
		}
		if _, err := e.Resume(ctx, id); err != nil {
			return recovered, fmt.Errorf("resume %q: %w", id, err)
		}
		recovered = append(recovered, id)
	}
	return recovered, nil
}

// Cancel 请求取消一个运行中的实例。返回错误仅表示该实例当前不在运行。
// 已在执行的任务会收到 ctx 取消；尚未开始的任务标记为 canceled，不算成功。
func (e *Engine) Cancel(workflowID string) error {
	e.mu.Lock()
	c, ok := e.cancels[workflowID]
	e.mu.Unlock()
	if !ok {
		return &ExecError{Code: ErrCodeCanceled, Detail: "workflow is not running: " + workflowID}
	}
	c()
	return nil
}

// GetState 读取实例当前持久化状态；不存在返回 (nil, nil)。
func (e *Engine) GetState(ctx context.Context, workflowID string) (*State, error) {
	return e.store.Load(ctx, workflowID)
}

// sleepCtx 在 ctx 取消时提前返回 false。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
