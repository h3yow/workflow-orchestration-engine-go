package workflow

import (
	"fmt"
	"reflect"
	"strings"
)

// Condition is a deterministic predicate evaluated before a task runs.
//
// Supported operators (Op):
//
//	"truthy"  - referenced value exists and is truthy (Ref, Value ignored)
//	"falsy"   - referenced value is absent or falsy
//	"exists"  - referenced path exists (Ref, Value ignored)
//	"eq"      - referenced value equals Value (deep equality)
//	"ne"      - referenced value differs from Value
//
// Ref is a dot-separated path beginning with a task ID, e.g.
// "build.artifact.version" looks up outputs["build"].artifact.version.
type Condition struct {
	Op    string `json:"op"`
	Ref   string `json:"ref"`
	Value any    `json:"value,omitempty"`
}

func (c *Condition) validate() error {
	if c == nil {
		return nil
	}
	switch c.Op {
	case "truthy", "falsy", "exists":
		if c.Ref == "" {
			return fmt.Errorf("%w: condition op %q requires ref", ErrInvalidConfig, c.Op)
		}
	case "eq", "ne":
		if c.Ref == "" {
			return fmt.Errorf("%w: condition op %q requires ref", ErrInvalidConfig, c.Op)
		}
	default:
		return fmt.Errorf("%w: unknown condition op %q", ErrInvalidConfig, c.Op)
	}
	if strings.HasPrefix(c.Ref, ".") || c.Ref == "" {
		return fmt.Errorf("%w: condition ref %q must start with a task id", ErrInvalidConfig, c.Ref)
	}
	return nil
}

// Evaluate evaluates the condition against succeeded-task outputs.
// "exists"/"truthy"/"falsy" never fail on missing paths; "eq"/"ne" on a
// missing path compare against nil.
func (c *Condition) Evaluate(outputs map[string]any) (bool, error) {
	if c == nil {
		return true, nil
	}
	root, path, err := splitRef(c.Ref)
	if err != nil {
		return false, err
	}
	got, present := lookupPath(outputs[root], path)
	switch c.Op {
	case "exists":
		return present, nil
	case "truthy":
		return present && isTruthy(got), nil
	case "falsy":
		return !present || !isTruthy(got), nil
	case "eq":
		return present && reflect.DeepEqual(normalize(got), normalize(c.Value)), nil
	case "ne":
		return !(present && reflect.DeepEqual(normalize(got), normalize(c.Value))), nil
	default:
		return false, fmt.Errorf("%w: unknown condition op %q", ErrInvalidConfig, c.Op)
	}
}

func splitRef(ref string) (root string, path []string, err error) {
	parts := strings.Split(ref, ".")
	if parts[0] == "" {
		return "", nil, fmt.Errorf("%w: invalid ref %q", ErrInvalidConfig, ref)
	}
	if len(parts) == 1 {
		return parts[0], nil, nil
	}
	return parts[0], parts[1:], nil
}

// lookupPath walks map/slice structures. Slice indices must be decimal digits.
func lookupPath(v any, path []string) (any, bool) {
	cur := v
	for _, seg := range path {
		switch node := cur.(type) {
		case map[string]any:
			next, ok := node[seg]
			if !ok {
				return nil, false
			}
			cur = next
		default:
			return nil, false
		}
	}
	return cur, true
}

func isTruthy(v any) bool {
	if v == nil {
		return false
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Bool:
		return rv.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int() != 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint() != 0
	case reflect.Float32, reflect.Float64:
		return rv.Float() != 0
	case reflect.String, reflect.Slice, reflect.Array, reflect.Map:
		return rv.Len() > 0
	case reflect.Ptr, reflect.Interface:
		return !rv.IsNil()
	default:
		return true
	}
}

// normalize makes JSON-decoded numbers comparable to literal values: JSON
// yields float64 for every number.
func normalize(v any) any {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case float64:
		return n
	default:
		return v
	}
}
