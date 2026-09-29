package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Store 工作流实例状态的持久化存储抽象（本地 JSON 文件实现）。
type Store interface {
	// Load 读取实例状态；不存在返回 (nil, nil)。
	Load(ctx context.Context, workflowID string) (*State, error)
	// Save 原子写入实例状态（先写临时文件再 rename，保证崩溃不产生半截文件）。
	Save(ctx context.Context, st *State) error
	// List 列出全部已持久化实例 ID（按字典序）。
	List(ctx context.Context) ([]string, error)
}

// fileStore 基于本地文件系统的 JSON 状态存储。
type fileStore struct {
	dir string
}

// NewFileStore 在 dir 下创建 JSON 文件存储（目录不存在会自动创建）。
func NewFileStore(dir string) (Store, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("state dir is empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	return &fileStore{dir: dir}, nil
}

func (s *fileStore) path(workflowID string) string {
	return filepath.Join(s.dir, url.PathEscape(workflowID)+".json")
}

func (s *fileStore) Load(_ context.Context, workflowID string) (*State, error) {
	data, err := os.ReadFile(s.path(workflowID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load state %q: %w", workflowID, err)
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, &ExecError{Code: ErrCodeStateCorrupt, Detail: err.Error()}
	}
	if st.Tasks == nil {
		st.Tasks = map[string]*TaskState{}
	}
	return &st, nil
}

func (s *fileStore) Save(_ context.Context, st *State) error {
	if st == nil {
		return errors.New("nil state")
	}
	st.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	final := s.path(st.WorkflowID)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write tmp state: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename state file: %w", err)
	}
	return nil
}

func (s *fileStore) List(_ context.Context) ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("list state dir: %w", err)
	}
	var ids []string
	for _, en := range entries {
		name := en.Name()
		if en.IsDir() || !strings.HasSuffix(name, ".json") || strings.HasSuffix(name, ".tmp") {
			continue
		}
		escaped := strings.TrimSuffix(name, ".json")
		id, err := url.PathUnescape(escaped)
		if err != nil {
			continue // 跳过无法解析的文件
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

// newStateFromDefinition 依据定义构造一个全新的实例状态（所有任务 pending）。
func newStateFromDefinition(def *Definition, workflowID string) *State {
	now := time.Now().UTC()
	st := &State{
		WorkflowID:     workflowID,
		DefinitionName: def.Name,
		Status:         WFPending,
		Tasks:          make(map[string]*TaskState, len(def.Tasks)),
		TaskOrder:      make([]string, 0, len(def.Tasks)),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	for _, t := range def.Tasks {
		task := t // 复制，避免引用遍历变量
		st.Tasks[task.ID] = &TaskState{
			ID:         task.ID,
			Definition: task,
			Status:     StatusPending,
		}
		st.TaskOrder = append(st.TaskOrder, task.ID)
	}
	return st
}

// cloneState 深拷贝状态，供调度协程在内存中演进后整体持久化。
func cloneState(st *State) *State {
	data, _ := json.Marshal(st)
	var cp State
	_ = json.Unmarshal(data, &cp)
	return &cp
}
