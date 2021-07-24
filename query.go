package squery

import (
	"errors"
	"strconv"
	"strings"
)

type Selector interface {
	Builder
	Select(cols ...Stringer) (string, []interface{}, error)
	RawSelect(cols ...string) (string, []interface{}, error)
	Count() (string, []interface{}, error)
}

// Query builder
type Query interface {
	Selector
	From(name Stringer) Query
	Where(expr Expression) Query
	Having(expr Expression) Query
	Columns(cols ...Stringer) Query
	RawColumns(cols ...string) Query
	Limit(n int64) Query
	Offset(n int64) Query
	One() Query
	OrderBy(clause Stringer) Query
	GroupBy(clause Stringer) Query
}

type query struct {
	from        Stringer
	whereExprs  []Expression
	havingExprs []Expression
	limit       int64
	offset      int64
	orderBy     Stringer
	groupBy     Stringer
	cols        []Stringer
}

// NewQuery create query builder
func NewQuery() Query {
	return &query{}
}

func (q *query) build(sb StringBuilder, ph Placeholder, isCount bool, cols ...Stringer) ([]interface{}, error) {
	if q.from == nil {
		return nil, errors.New("FROM clause can not be empty")
	}
	var args []interface{}
	sb.WriteString("SELECT ")
	if len(cols) == 0 {
		sb.WriteString("*")
	} else {
		sb.WriteString(cols[0].String())
		for idx := 1; idx < len(cols); idx++ {
			sb.WriteByte(bComma)
			sb.WriteString(cols[idx].String())
		}
	}
	sb.WriteString(" FROM ")
	sb.WriteString(q.from.String())
	if len(q.whereExprs) > 0 {
		addWhere := true
		for _, e := range q.whereExprs {
			if e.IsEmpty() {
				continue
			}
			if addWhere {
				addWhere = false
				sb.WriteString(" WHERE ")
			}
			varg, err := e.Build(sb, ph)
			if err != nil {
				return nil, err
			}
			args = append(args, varg...)
		}
	}

	// Add group by if not SELECT COUNT(*)
	if !isCount {
		// For select count, we do need limit, offset, order by
		if q.groupBy != nil {
			sb.WriteString(" GROUP BY")
			sb.WriteString(q.groupBy.String())
		}
	}

	// process HAVING clause
	if len(q.havingExprs) > 0 {
		addHaving := true
		for _, e := range q.havingExprs {
			if e.IsEmpty() {
				continue
			}
			if addHaving {
				addHaving = false
				sb.WriteString(" HAVING ")
			}
			varg, err := e.Build(sb, ph)
			if err != nil {
				return nil, err
			}
			args = append(args, varg...)
		}
	}

	// ADD ORDER BY, limit and offset if not count(*)
	if !isCount {
		if q.orderBy != nil {
			sb.WriteString(" ORDER BY ")
			sb.WriteString(q.orderBy.String())
		}

		if q.limit > 0 {
			sb.WriteString(" LIMIT ")
			sb.WriteString(strconv.FormatInt(q.limit, 10))
		}

		if q.offset > 0 {
			sb.WriteString(" OFFSET ")
			sb.WriteString(strconv.FormatInt(q.offset, 10))
		}
	}

	if ph.Position() != len(args) {
		return nil, errors.New("number of placeholder do not match arguments count")
	}

	return args, nil
}

func (q *query) IsEmpty() bool {
	return q.from == nil || q.from.String() == ""
}

func (q *query) Build(sb StringBuilder, ph Placeholder) ([]interface{}, error) {
	return q.build(sb, ph, false)
}
func (q *query) From(name Stringer) Query {
	q.from = name
	return q
}

func (q *query) Where(expr Expression) Query {
	q.whereExprs = append(q.whereExprs, expr)
	return q
}
func (q *query) Having(expr Expression) Query {
	q.havingExprs = append(q.havingExprs, expr)
	return q
}
func (q *query) Columns(cols ...Stringer) Query {
	q.cols = append(q.cols, cols...)
	return q
}
func (q *query) RawColumns(cols ...string) Query {
	q.cols = append(q.cols, SSliceFrom(cols)...)
	return q
}
func (q *query) One() Query {
	q.limit = 1
	return q
}
func (q *query) Limit(n int64) Query {
	if n <= 0 {
		panic("limit must be greater than 0")
	}
	q.limit = n
	return q
}

func (q *query) Offset(n int64) Query {
	if n < 0 {
		// TODO: panic or error?
		n = 0
	}
	q.offset = n
	return q
}
func (q *query) OrderBy(s Stringer) Query {
	q.orderBy = s
	return q
}
func (q *query) GroupBy(s Stringer) Query {
	q.groupBy = s
	return q
}
func (q *query) RawSelect(cols ...string) (string, []interface{}, error) {
	return q.Select(SSliceFrom(cols)...)
}
func (q *query) Select(cols ...Stringer) (string, []interface{}, error) {
	selectCols := q.cols
	if len(cols) != 0 {
		selectCols = cols
	}
	sb := strings.Builder{}
	ph := NewPsqlPlaceholder()
	args, err := q.build(&sb, ph, false, selectCols...)
	if err != nil {
		return "", nil, err
	}
	return sb.String(), args, nil
}

func (q *query) Count() (string, []interface{}, error) {
	sb := strings.Builder{}
	ph := NewPsqlPlaceholder()
	args, err := q.build(&sb, ph, true, R("COUNT(*)"))
	if err != nil {
		return "", nil, err
	}
	return sb.String(), args, nil
}
