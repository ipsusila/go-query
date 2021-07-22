package query

import "strings"

// Ensure SQL expression implement Expression interface
var _ Expression = (*SqlExpression)(nil)

// FnMapField functions maps field from one to another
type FnMapField func(string) (string, error)

// SqlExpression includes clause, and args
type SqlExpression struct {
	Clause    string        `json:"clause"`
	Args      []interface{} `json:"args"`
	Fields    []string      `json:"fields"`
	SqlFields []string      `json:"sqlFields"`

	fm FnMapField
	ph Placeholder
}

func (se *SqlExpression) mappedField(term string) (string, error) {
	if se.fm == nil {
		return term, nil
	}

	return se.fm(term)
}

// Builder interface, so that it can be passed to query
func (se *SqlExpression) Build(sb *strings.Builder, ph Placeholder) ([]interface{}, error) {
	sb.WriteString(se.Clause)
	return se.Args, nil
}
