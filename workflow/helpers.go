package workflow

import "strings"

// condName 若表达式为具名条件 "cond:xxx"，返回 xxx；否则返回空串。
func condName(expr string) string {
	if strings.HasPrefix(expr, "cond:") {
		return strings.TrimPrefix(expr, "cond:")
	}
	return ""
}

// exprTaskStatus 解析 "taskID:status"，返回任务 ID 与目标状态。
func exprTaskStatus(expr string) (string, TaskStatus) {
	idx := strings.IndexByte(expr, ':')
	if idx <= 0 {
		return "", ""
	}
	return expr[:idx], TaskStatus(expr[idx+1:])
}
