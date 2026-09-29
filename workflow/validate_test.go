package workflow

import "testing"

func TestValidateErrors(t *testing.T) {
	base := func() *Definition {
		return &Definition{
			Name: "d",
			Tasks: []TaskDefinition{
				{ID: "a", Handler: "h"},
				{ID: "b", Handler: "h", DependsOn: []string{"a"}},
			},
		}
	}

	cases := []struct {
		name string
		mut  func(d *Definition)
		code DefinitionErrorCode
	}{
		{"empty name", func(d *Definition) { d.Name = "" }, ErrCodeInvalidConfig},
		{"no tasks", func(d *Definition) { d.Tasks = nil }, ErrCodeInvalidConfig},
		{"empty task id", func(d *Definition) { d.Tasks[0].ID = "" }, ErrCodeInvalidConfig},
		{"empty handler", func(d *Definition) { d.Tasks[0].Handler = "" }, ErrCodeInvalidConfig},
		{"bad retry attempts", func(d *Definition) { d.Tasks[0].Retry = &RetryPolicy{MaxAttempts: -1} }, ErrCodeInvalidConfig},
		{"backoff order", func(d *Definition) {
			d.Tasks[0].Retry = &RetryPolicy{MaxAttempts: 3, InitialBackoff: 10, MaxBackoff: 1}
		}, ErrCodeInvalidConfig},
		{"bad condition syntax", func(d *Definition) { d.Tasks[0].Condition = "weird" }, ErrCodeInvalidConfig},
		{"bad condition status", func(d *Definition) { d.Tasks[0].Condition = "a:bogus" }, ErrCodeInvalidConfig},
		{"duplicate dep", func(d *Definition) { d.Tasks[1].DependsOn = []string{"a", "a"} }, ErrCodeInvalidConfig},
		{"duplicate id", func(d *Definition) { d.Tasks[1].ID = "a" }, ErrCodeDuplicateID},
		{"missing dep", func(d *Definition) { d.Tasks[1].DependsOn = []string{"ghost"} }, ErrCodeMissingDependency},
		{"missing cond ref", func(d *Definition) { d.Tasks[1].Condition = "ghost:succeeded" }, ErrCodeMissingDependency},
		{"self cycle", func(d *Definition) { d.Tasks[0].DependsOn = []string{"a"} }, ErrCodeCyclicDependency},
		{"cycle a-b", func(d *Definition) {
			d.Tasks[0].DependsOn = []string{"b"} // b->a, a->b
		}, ErrCodeCyclicDependency},
		{"cycle 3 nodes", func(d *Definition) {
			d.Tasks = append(d.Tasks, TaskDefinition{ID: "c", Handler: "h", DependsOn: []string{"b"}})
			d.Tasks[0].DependsOn = []string{"c"} // a->c->b->a
		}, ErrCodeCyclicDependency},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := base()
			tc.mut(d)
			err := Validate(d)
			if err == nil {
				t.Fatalf("expected error code %s, got nil", tc.code)
			}
			de, ok := err.(*DefinitionError)
			if !ok {
				t.Fatalf("expected *DefinitionError, got %T: %v", err, err)
			}
			if de.Code != tc.code {
				t.Fatalf("code=%s want %s (msg=%s)", de.Code, tc.code, de.Message)
			}
		})
	}
}

func TestValidateAcceptsDiamond(t *testing.T) {
	d := &Definition{
		Name: "diamond",
		Tasks: []TaskDefinition{
			{ID: "root", Handler: "h"},
			{ID: "l", Handler: "h", DependsOn: []string{"root"}},
			{ID: "r", Handler: "h", DependsOn: []string{"root"}},
			{ID: "leaf", Handler: "h", DependsOn: []string{"l", "r"}},
		},
	}
	if err := Validate(d); err != nil {
		t.Fatalf("diamond should be valid: %v", err)
	}
}
