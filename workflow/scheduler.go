package workflow

import "context"

// event 是工作 goroutine 回传调度循环的唯一通道。
type event struct {
	kind    eventKind
	taskID  string
	ts      *TaskState       // worker 私有副本的最新任务状态
	spawned []TaskDefinition // 仅 finished + succeeded 时携带动态任务
	ack     chan struct{}    // 仅 progress 携带：状态落盘后关闭，worker 才开始执行
}

type eventKind string

const (
	evProgress eventKind = "progress" // running/ready 心跳，仅用于持久化
	evFinished eventKind = "finished" // 终态
)

// execInput 是下发给 worker 的不可变输入（快照），避免共享内存竞争。
type execInput struct {
	ts      *TaskState
	results map[string]map[string]any // 依赖任务结果快照
}

// run 驱动一个工作流实例直到终态。
// 任务定义以持久化在各 TaskState 中的副本为准（含动态生成任务），
// 因此全新执行与崩溃恢复走同一套调度逻辑。
func (e *Engine) run(parent context.Context, st *State) (*State, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	wfID := st.WorkflowID
	ctx = WithWorkflowID(ctx, wfID)

	e.mu.Lock()
	e.cancels[wfID] = cancel
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.cancels, wfID)
		e.mu.Unlock()
	}()

	st.Status = WFRunning
	if err := e.store.Save(ctx, st); err != nil {
		return nil, err
	}
	e.logger.Info(ctx, "workflow started", "tasks", len(st.TaskOrder))

	// 不设置 MaxConcurrency 时不使用信号量（unlimited），
	// 动态 spawn 增加任务也不会形成人为瓶颈。
	unlimited := e.cfg.MaxConcurrency <= 0
	semSize := e.cfg.MaxConcurrency
	if !unlimited && semSize <= 0 {
		semSize = 1
	}
	sem := make(chan struct{}, semSize)
	events := make(chan event, 32)
	running := map[string]bool{}

	for {
		// 优先处理工作流级取消（用户取消或失败触发的 cancel()），
		// 避免 select 先取到在途任务的终态事件而漏标工作流状态。
		if ctx.Err() != nil && st.Status != WFFailed {
			st.Status = WFCanceled
			if st.FailureCause == "" {
				st.FailureCause = "workflow canceled by caller"
			}
			e.logger.Warn(ctx, "workflow context canceled", "cause", st.FailureCause)
		}

		e.dispatch(ctx, st, sem, events, running, unlimited)

		if len(running) == 0 {
			if !isWorkflowTerminal(st) {
				// 防御性：正常校验过的图不会走到这里。
				e.abortRemaining(ctx, st, running, "unreachable tasks: dependencies cannot be satisfied")
				cancel()
			}
			break
		}

		select {
		case ev := <-events:
			st.Tasks[ev.taskID] = ev.ts
			if ev.kind == evProgress {
				// 进行中心跳（running）：不释放并发槽、不清 running；
				// 落盘后再放行 worker，保证"开始执行"这一事实在崩溃前已持久化。
				_ = e.store.Save(ctx, st)
				close(ev.ack)
				continue
			}

			delete(running, ev.taskID)
			if !unlimited {
				<-sem // 任务终态后才归还并发槽
			}
			e.logger.Info(ctx, "task finished",
				"task_id", ev.taskID, "status", ev.ts.Status, "attempts", ev.ts.Attempt, "err", ev.ts.Err)

			if ev.ts.Status == StatusSucceeded && len(ev.spawned) > 0 {
				if cause := e.integrateSpawned(ctx, st, ev); cause != "" {
					e.markFailed(ctx, st, cause)
					cancel()
				}
			}
			if ev.ts.Status == StatusFailed {
				if ev.ts.Definition.FailureBranch {
					// 失败分支：保留 failed 终态，调度继续，由条件门控选择下游。
					e.logger.Info(ctx, "failure-branch task failed, continuing", "task_id", ev.taskID)
				} else {
					e.markFailed(ctx, st, "task failed: "+ev.taskID+": "+ev.ts.Err)
					cancel() // 让在途兄弟任务尽快终止
				}
			}
			// 在途任务因工作流取消而终止：确保工作流状态被标记。
			if ev.ts.Status == StatusCanceled && ctx.Err() != nil && st.Status != WFFailed {
				st.Status = WFCanceled
				if st.FailureCause == "" {
					st.FailureCause = "workflow canceled by caller"
				}
			}
			_ = e.store.Save(ctx, st)

		case <-ctx.Done():
			// 用户取消（失败导致的 cancel 已通过 markFailed 设置状态）。
			if st.Status != WFFailed {
				st.Status = WFCanceled
				st.FailureCause = "workflow canceled by caller"
			}
			e.logger.Warn(ctx, "workflow context canceled", "cause", st.FailureCause)
		}
	}

	e.finalize(st)
	if err := e.store.Save(ctx, st); err != nil {
		return st, err
	}
	e.logger.Info(ctx, "workflow finished", "status", st.Status, "failure_cause", st.FailureCause)
	return cloneState(st), nil
}

// dispatch 扫描全部 pending 任务：满足门控条件的则下发执行，不满足的标记 skipped，
// 工作流已进入失败/取消时剩余任务统一标记 canceled。
func (e *Engine) dispatch(ctx context.Context, st *State, sem chan struct{}, events chan event, running map[string]bool, unlimited bool) {
	aborting := st.Status == WFFailed || st.Status == WFCanceled
	for _, id := range st.TaskOrder {
		ts := st.Tasks[id]
		if ts.Status.Terminal() || running[id] {
			continue
		}

		if aborting {
			if running[id] {
				continue // 在途任务由 worker 回传自己的终态，不在此覆盖
			}
			ts.Status = StatusCanceled
			ts.Err = st.FailureCause
			continue
		}

		allTerminal := true
		for _, d := range ts.Definition.DependsOn {
			dep, ok := st.Tasks[d]
			if !ok || !dep.Status.Terminal() {
				allTerminal = false
				break
			}
		}
		if !allTerminal {
			continue
		}

		run := e.decide(ts, st)
		td := ts.Definition
		if !run {
			ts.Status = StatusSkipped
			if td.Condition == "" {
				ts.Err = "skipped: one or more dependencies did not succeed"
			} else {
				ts.Err = "skipped: condition not satisfied: " + td.Condition
			}
			e.logger.Info(ctx, "task skipped by gate", "task_id", id, "condition", td.Condition)
			continue
		}

		if !unlimited {
			select {
			case sem <- struct{}{}:
			default:
				continue // 并发槽位已满，等任务结束后再试
			}
		}
		running[id] = true
		ts.Status = StatusReady
		e.persist(ctx, st)
		e.logger.Info(ctx, "task dispatched", "task_id", id, "next_attempt", ts.Attempt+1, "deps", ts.Definition.DependsOn)

		in := execInput{ts: cloneTaskState(ts), results: e.depResults(ts, st)}
		go e.execute(ctx, in, events)
	}
}

// decide 此时所有依赖均已终态：无条件时要求所有依赖 succeeded；
// 有条件时按表达式或具名函数求值。
func (e *Engine) decide(ts *TaskState, st *State) bool {
	td := ts.Definition
	if td.Condition == "" {
		for _, d := range td.DependsOn {
			if st.Tasks[d].Status != StatusSucceeded {
				return false
			}
		}
		return true
	}
	if name := condName(td.Condition); name != "" {
		f, ok := e.lookupCondition(name)
		if !ok {
			return false
		}
		snapshot := make(map[string]*TaskState, len(st.Tasks))
		for k, v := range st.Tasks {
			snapshot[k] = cloneTaskState(v)
		}
		return f(snapshot)
	}
	refID, want := exprTaskStatus(td.Condition)
	ref, ok := st.Tasks[refID]
	if !ok {
		return false
	}
	return ref.Status == want
}

func (e *Engine) depResults(ts *TaskState, st *State) map[string]map[string]any {
	out := make(map[string]map[string]any, len(ts.Definition.DependsOn))
	for _, d := range ts.Definition.DependsOn {
		if dep := st.Tasks[d]; dep != nil && dep.Result != nil {
			cp := make(map[string]any, len(dep.Result))
			for k, v := range dep.Result {
				cp[k] = v
			}
			out[d] = cp
		}
	}
	return out
}

// integrateSpawned 校验并接入动态任务：重复标识、缺失依赖、循环依赖、
// 未注册处理器一律拒绝，返回非空 cause（调用方据此令工作流失败）。
func (e *Engine) integrateSpawned(ctx context.Context, st *State, ev event) string {
	seen := map[string]struct{}{}
	var added []TaskDefinition
	for _, td := range ev.spawned {
		if td.ID == "" || td.Handler == "" {
			return "invalid dynamic task: empty id or handler (spawned by " + ev.taskID + ")"
		}
		if _, dup := st.Tasks[td.ID]; dup {
			return "dynamic task id conflict: " + td.ID
		}
		if _, dup := seen[td.ID]; dup {
			return "dynamic task id conflict within batch: " + td.ID
		}
		if _, ok := e.lookupHandler(td.Handler); !ok {
			return "dynamic task handler not registered: " + td.Handler
		}
		seen[td.ID] = struct{}{}
		added = append(added, td)
	}
	// 构造含新任务的临时定义，复用环/缺失依赖校验。
	tmp := &Definition{Name: st.DefinitionName + ":dynamic"}
	for _, id := range st.TaskOrder {
		tmp.Tasks = append(tmp.Tasks, st.Tasks[id].Definition)
	}
	for _, td := range added {
		td.Dynamic = true
		tmp.Tasks = append(tmp.Tasks, td)
	}
	if err := Validate(tmp); err != nil {
		return "invalid dynamic tasks: " + err.Error()
	}
	for _, td := range added {
		td.Dynamic = true
		st.Tasks[td.ID] = &TaskState{ID: td.ID, Definition: td, Status: StatusPending}
		st.TaskOrder = append(st.TaskOrder, td.ID)
		e.logger.Info(ctx, "dynamic task accepted", "task_id", td.ID, "spawned_by", ev.taskID)
	}
	return ""
}

func (e *Engine) persist(ctx context.Context, st *State) {
	if err := e.store.Save(ctx, st); err != nil {
		e.logger.Error(ctx, "persist failed", "err", err)
	}
}

func (e *Engine) markFailed(ctx context.Context, st *State, cause string) {
	if st.Status != WFFailed && st.Status != WFCanceled {
		st.Status = WFFailed
		st.FailureCause = cause
		e.logger.Warn(ctx, "workflow marked failed", "cause", cause)
	}
}

// abortRemaining 防御性收尾：把所有未终态任务标记 canceled。
func (e *Engine) abortRemaining(ctx context.Context, st *State, running map[string]bool, cause string) {
	if st.Status != WFFailed && st.Status != WFCanceled {
		st.Status = WFFailed
		st.FailureCause = cause
	}
	for _, id := range st.TaskOrder {
		ts := st.Tasks[id]
		if !ts.Status.Terminal() && !running[id] {
			ts.Status = StatusCanceled
			ts.Err = cause
		}
	}
	e.logger.Warn(ctx, "workflow aborting", "cause", cause)
}

func isWorkflowTerminal(st *State) bool {
	if st.Status == WFFailed || st.Status == WFCanceled {
		// 失败/取消也需等所有在途任务结束，由 running 计数控制；
		// 但如果已经没有在途任务，则立即终态。
		for _, ts := range st.Tasks {
			if ts.Status == StatusRunning || ts.Status == StatusReady {
				return false
			}
		}
		return true
	}
	for _, ts := range st.Tasks {
		if !ts.Status.Terminal() {
			return false
		}
	}
	return true
}

func (e *Engine) finalize(st *State) {
	if st.Status == WFFailed || st.Status == WFCanceled {
		for _, ts := range st.Tasks {
			if !ts.Status.Terminal() {
				ts.Status = StatusCanceled
				if ts.Err == "" {
					ts.Err = st.FailureCause
				}
			}
		}
		return
	}
	// 所有任务已终态。仅当存在"非失败分支"任务失败时才算工作流失败；
	// 标记 FailureBranch 的失败是被显式处理的分支输入，工作流仍可成功。
	for _, ts := range st.Tasks {
		if ts.Status == StatusFailed && !ts.Definition.FailureBranch {
			st.Status = WFFailed
			if st.FailureCause == "" {
				st.FailureCause = "task failed: " + ts.ID + ": " + ts.Err
			}
			return
		}
	}
	st.Status = WFSucceeded
}

func cloneTaskState(ts *TaskState) *TaskState {
	cp := *ts
	cp.Records = append([]RetryRecord(nil), ts.Records...)
	cp.Definition.DependsOn = append([]string(nil), ts.Definition.DependsOn...)
	if ts.Definition.Params != nil {
		cp.Definition.Params = cloneMap(ts.Definition.Params)
	}
	if ts.Result != nil {
		cp.Result = cloneMap(ts.Result)
	}
	return &cp
}

func cloneMap(m map[string]any) map[string]any {
	cp := make(map[string]any, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
