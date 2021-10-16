package squery

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
)

// FieldValues stores filed: value for db
type FieldValues map[string]interface{}

type templateQuery struct {
	selTpl      string
	cntTpl      string
	fm          FnMapField
	fv          FieldValues
	whereExprs  []Expression
	havingExprs []Expression
	limit       int64
	offset      int64
	orderBy     Stringer
	groupBy     Stringer
	cols        []Stringer
}

// NewTemplateQuery create query builder
func NewTemplateQuery(selTpl, cntTpl string, fm FnMapField, fv FieldValues) Query {
	return &templateQuery{
		selTpl: selTpl,
		cntTpl: cntTpl,
		fm:     fm,
		fv:     fv,
	}
}

func (q *templateQuery) join(cols ...Stringer) string {
	var sb strings.Builder
	if len(cols) == 0 {
		sb.WriteString("*")
	} else {
		sb.WriteString(cols[0].String())
		for idx := 1; idx < len(cols); idx++ {
			sb.WriteByte(bComma)
			sb.WriteString(cols[idx].String())
		}
	}
	return sb.String()
}

func (q *templateQuery) build(qTpl string, ph Placeholder, isCount bool, cols ...Stringer) (string, []interface{}, error) {
	// BUILD SELECT

	// store arguments
	var args []interface{}

	// select template
	query := strings.TrimSpace(qTpl)
	// handle only SELECT or WITH query
	if len(query) < 6 {
		return "", nil, errors.New("query template too sort")
	}
	sSelect := query[:6]
	sWith := query[:4]
	if !strings.EqualFold(sSelect, tSelect) && !strings.EqualFold(sWith, tWith) {
		return "", nil, errors.New("query must begin with SELECT/WITH")
	}

	// 1. replace {{field}} and {{field_value}}
	for field, value := range q.fv {
		tplField := "{{" + field + "}}"
		if !strings.Contains(query, tplField) {
			continue
		}

		dbField, err := q.fm(field)
		if err != nil {
			return "", nil, err
		}

		// construct place holder
		tplFieldVal := "{{" + field + "_value}}"
		var tplFieldValPh string

		// check wether array or not
		rt := reflect.TypeOf(value)
		switch rt.Kind() {
		case reflect.Slice, reflect.Array:
			s := reflect.ValueOf(value)
			if nelem := s.Len(); nelem > 0 {
				v := s.Index(0)
				if !v.CanInterface() {
					return "", nil, errors.New("invalid field value")
				}
				args = append(args, v.Interface())

				sb := strings.Builder{}
				sb.WriteByte(bLParenthesis)
				sb.WriteString(ph.Next())
				for i := 1; i < nelem; i++ {
					sb.WriteByte(bComma)
					sb.WriteString(ph.Next())

					v := s.Index(i)
					if !v.CanInterface() {
						return "", nil, errors.New("invalid field value")
					}
					args = append(args, v.Interface())
				}
				sb.WriteByte(bRParenthesis)

				tplFieldValPh = sb.String()
			}

		default:
			tplFieldValPh = ph.Next()
			args = append(args, value)
		}

		query = strings.ReplaceAll(query, tplField, dbField)
		query = strings.ReplaceAll(query, tplFieldVal, tplFieldValPh)
	}

	// relace {{columns}}
	strCols := q.join(cols...)
	query = strings.ReplaceAll(query, tColumns, strCols)

	// replace where
	if nexp := len(q.whereExprs); nexp > 0 {
		sb := strings.Builder{}
		for idx, e := range q.whereExprs {
			if e.IsEmpty() {
				continue
			}
			if idx > 0 {
				sb.WriteString(" AND ")
			}

			if nexp > 1 {
				sb.WriteByte(bLParenthesis)
			}
			varg, err := e.Build(&sb, ph)
			if err != nil {
				return "", nil, err
			}
			if nexp > 1 {
				sb.WriteByte(bRParenthesis)
			}
			args = append(args, varg...)
		}

		// replace where
		query = strings.ReplaceAll(query, tWhere, sb.String())
	}

	// Add group by if not SELECT COUNT(*)
	if q.groupBy != nil {
		sb := strings.Builder{}
		sb.WriteString(" GROUP BY ")
		sb.WriteString(q.groupBy.String())
		sb.WriteByte(bSpace)
		query = strings.ReplaceAll(query, tGroupBy, sb.String())
	} else {
		query = strings.ReplaceAll(query, tGroupBy, "")
	}

	// process HAVING clause
	if nexp := len(q.havingExprs); nexp > 0 {
		sb := strings.Builder{}
		for idx, e := range q.havingExprs {
			if e.IsEmpty() {
				continue
			}
			if idx > 0 {
				sb.WriteString(" AND ")
			}

			if nexp > 1 {
				sb.WriteByte(bLParenthesis)
			}
			varg, err := e.Build(&sb, ph)
			if err != nil {
				return "", nil, err
			}
			if nexp > 1 {
				sb.WriteByte(bRParenthesis)
			}
			args = append(args, varg...)
		}
		query = strings.ReplaceAll(query, tHaving, sb.String())
	}

	// ADD ORDER BY, limit and offset if not count(*)
	if isCount {
		query = strings.ReplaceAll(query, tOrderBy, "")
		query = strings.ReplaceAll(query, tLimit, "")
		query = strings.ReplaceAll(query, tOffset, "")
	} else {
		if q.orderBy != nil {
			sb := strings.Builder{}
			sb.WriteString(" ORDER BY ")
			sb.WriteString(q.orderBy.String())
			sb.WriteByte(bSpace)
			query = strings.ReplaceAll(query, tOrderBy, sb.String())
		} else {
			query = strings.ReplaceAll(query, tOrderBy, "")
		}

		if q.limit > 0 {
			sb := strings.Builder{}
			sb.WriteString(" LIMIT ")
			sb.WriteString(strconv.FormatInt(q.limit, 10))
			sb.WriteByte(bSpace)
			query = strings.ReplaceAll(query, tLimit, sb.String())
		} else {
			query = strings.ReplaceAll(query, tLimit, "")
		}

		if q.offset > 0 {
			sb := strings.Builder{}
			sb.WriteString(" OFFSET ")
			sb.WriteString(strconv.FormatInt(q.offset, 10))
			sb.WriteByte(bSpace)
			query = strings.ReplaceAll(query, tOffset, sb.String())
		} else {
			query = strings.ReplaceAll(query, tOffset, "")
		}
	}

	if ph.Position() != len(args) {
		return "", nil, errors.New("number of placeholder do not match arguments count")
	}

	return query, args, nil
}

func (q *templateQuery) IsEmpty() bool {
	return q.selTpl == "" || (len(q.fv) > 0 && q.fm == nil)
}

func (q *templateQuery) Build(sb StringBuilder, ph Placeholder) ([]interface{}, error) {
	query, args, err := q.build(q.selTpl, ph, false, q.cols...)
	if err != nil {
		return nil, err
	}
	sb.WriteString(query)
	return args, nil
}
func (q *templateQuery) From(name Stringer) Query {
	// DO NOTHING
	return q
}

func (q *templateQuery) Where(expr Expression) Query {
	q.whereExprs = append(q.whereExprs, expr)
	return q
}
func (q *templateQuery) Having(expr Expression) Query {
	q.havingExprs = append(q.havingExprs, expr)
	return q
}
func (q *templateQuery) Columns(cols ...Stringer) Query {
	q.cols = append(q.cols, cols...)
	return q
}
func (q *templateQuery) RawColumns(cols ...string) Query {
	q.cols = append(q.cols, SSliceFrom(cols)...)
	return q
}
func (q *templateQuery) One() Query {
	q.limit = 1
	return q
}
func (q *templateQuery) Limit(n int64) Query {
	if n <= 0 {
		panic("limit must be greater than 0")
	}
	q.limit = n
	return q
}

func (q *templateQuery) Offset(n int64) Query {
	if n < 0 {
		// TODO: panic or error?
		n = 0
	}
	q.offset = n
	return q
}
func (q *templateQuery) OrderBy(s Stringer) Query {
	q.orderBy = s
	return q
}
func (q *templateQuery) GroupBy(s Stringer) Query {
	q.groupBy = s
	return q
}
func (q *templateQuery) RawSelect(cols ...string) (string, []interface{}, error) {
	return q.Select(SSliceFrom(cols)...)
}
func (q *templateQuery) Select(cols ...Stringer) (string, []interface{}, error) {
	selectCols := q.cols
	if len(cols) != 0 {
		selectCols = cols
	}
	ph := NewPsqlPlaceholder()
	query, args, err := q.build(q.selTpl, ph, false, selectCols...)
	if err != nil {
		return "", nil, err
	}
	return query, args, nil
}

func (q *templateQuery) Count() (string, []interface{}, error) {
	ph := NewPsqlPlaceholder()
	qTpl := q.cntTpl
	if len(qTpl) == 0 {
		qTpl = q.selTpl
	}
	query, args, err := q.build(qTpl, ph, true, R("COUNT(*)"))
	if err != nil {
		return "", nil, err
	}
	return query, args, nil
}
