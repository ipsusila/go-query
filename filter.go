package squery

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

// Sorting order
const (
	AscendingOrder  = "ASC"
	DescendingOrder = "DESC"
)

// filter limit
var (
	MaxLimitPerPage     = int64(500)
	DefaultLimitPerPage = int64(25)
	AllMatchers         = []string{
		sqlLike,
		sqlNotLike,
		sqlILike,
		sqlNotILike,
		sqlSimilarTo,
		sqlNotSimilarTo,
		sqlRegexMatch,
		sqlIRegexMatch,
		sqlNotRegexMatch,
		sqlNotIRegexMatch,
	}
)

// SortConditions stores list of sort items
type SortConditions []*Sort

// SelectColumn definition for column mapping
// map between json->db->resultfield
type SelectColumn struct {
	JsonField   string
	SelectField string
	ResultField string
}

// Sort stores sort information
type Sort struct {
	Fields []string `json:"fields"`
	Order  string   `json:"order"`
}

// Pagination of the result
type Pagination struct {
	Page        int64   `json:"page"`    // starts from 0
	PerPage     int64   `json:"perPage"` // number of record in one page
	NextPageKey *string `json:"nextPageKey"`
	offset      int64
}

// EqFilter stores query term of "field=$value"
type EqFilter struct {
	Field string      `json:"field"`
	Value interface{} `json:"value"`
}

// QueryTErm stores query term with specific operator
type QueryTerm struct {
	Fields  []string `json:"fields,omitempty"`
	Matcher string   `json:"matcher"`
	Term    string   `json:"term"`
}

// RefSearchArg stores params for ref many
type RefSearchArg struct {
	Target     EqFilter       `json:"target"`
	Sorts      SortConditions `json:"sorts,omitempty"`
	Pagination *Pagination    `json:"pagination,omitempty"`
}

// ListSearchArg stores params for list searching
type ListSearchArg struct {
	Sorts      SortConditions  `json:"sorts,omitempty"`
	Pagination *Pagination     `json:"pagination,omitempty"`
	Filter     json.RawMessage `json:"filter"`
	Query      *QueryTerm      `json:"query,omitempty"`
	Fields     []string        `json:"fields,omitempty"`
}

// DataList for storing many/list query result
type DataList struct {
	Success    bool        `json:"success"`
	Total      int64       `json:"total"`
	DataCount  int         `json:"dataCount"`
	Pagination *Pagination `json:"pagination,omitempty"`
	Data       interface{} `json:"data"`
}

type queryTermExp struct {
	queryTerm    *QueryTerm
	fullTextCols []string
	fm           FnMapField
}

// NewQueryTermExpression convert query term to expression
func NewQueryTermExpression(q *QueryTerm, fulltextCols []string, fm FnMapField) Expression {
	return &queryTermExp{
		queryTerm:    q,
		fullTextCols: fulltextCols,
		fm:           fm,
	}
}

func (q *queryTermExp) IsEmpty() bool {
	return q.queryTerm == nil || q.queryTerm.IsEmpty()
}

func (q *queryTermExp) Build(sb StringBuilder, ph Placeholder) ([]interface{}, error) {
	if q.IsEmpty() {
		return nil, nil
	}

	// If fields is specified, otherwhise use fulltext cols
	columns := q.fullTextCols
	if len(q.queryTerm.Fields) != 0 {
		columns = q.queryTerm.Fields
	}

	// verify matchers
	matcher := ""
	for _, m := range AllMatchers {
		if strings.EqualFold(m, q.queryTerm.Matcher) {
			matcher = m
			break
		}
	}
	if matcher == "" {
		return nil, errors.New("valid matcher keyword not found")
	}

	numItem := 0
	args := []interface{}{}
	for _, col := range columns {
		if field, err := q.fm(col); err == nil {
			// construct clause, e.g. (name SIMILAR TO ?) OR (tile LIKE ?)
			if numItem > 0 {
				sb.WriteString(" OR ")
			}
			sb.WriteByte(bLParenthesis)
			sb.WriteString(field)
			sb.WriteByte(bSpace)
			sb.WriteString(matcher)
			sb.WriteByte(bSpace)
			sb.WriteString(ph.Next())
			sb.WriteByte(bRParenthesis)

			args = append(args, q.queryTerm.Term)
			numItem++
		}
	}
	return args, nil
}

func (q *QueryTerm) IsEmpty() bool {
	return strings.TrimSpace(q.Term) == "" || q.Matcher == ""
}

// Default value for pagination
func (l *ListSearchArg) DefaultPerPage(perPage int64) *ListSearchArg {
	if l.Pagination != nil {
		l.Pagination.PerPage = perPage
	} else {
		l.Pagination = &Pagination{PerPage: perPage}
	}
	l.Pagination.Page = 1
	return l
}

// IsFilterSpecified return true if Filte has valid json
func (l *ListSearchArg) IsFilterSpecified() bool {
	return len(l.Filter) > 0
}

// IsFilterEmpty return true if filter is not set
func (l *ListSearchArg) IsFilterEmpty() bool {
	return len(l.Filter) == 0
}

// IsZero return true if object not set
func (l *ListSearchArg) IsZero() bool {
	return len(l.Filter) == 0 &&
		l.Pagination == nil &&
		len(l.Sorts) == 0 &&
		l.Query == nil
}

// IsZeror return true if object is not initialized yet
func (r *RefSearchArg) IsZero() bool {
	return r.Pagination == nil &&
		len(r.Sorts) == 0 &&
		r.Target.Field == ""
}

// OrderString from sort arg
func (s *Sort) OrderString() string {
	if strings.EqualFold(s.Order, DescendingOrder) {
		return DescendingOrder
	}
	return AscendingOrder
}

// IsAscending return true if the data is sorted in ASCENDING order
func (s *Sort) IsAscending() bool {
	return s.OrderString() == AscendingOrder
}

// Clause return sorting clause, e.g. created_at ASC
func (s *Sort) Clause(jsToField map[string]string) string {
	fields := []string{}
	for _, field := range s.Fields {
		if dbField, ok := jsToField[field]; ok {
			fields = append(fields, strconv.Quote(dbField))
		}
	}
	if len(fields) == 0 {
		return ""
	}

	sb := strings.Builder{}
	sb.WriteByte(bLParenthesis)
	sb.WriteString(fields[0])
	for i := 1; i < len(fields); i++ {
		sb.WriteString(fields[i])
	}
	sb.WriteString(") ")
	sb.WriteString(s.OrderString())

	return sb.String()
}

// Convert to order by clause
func (sc SortConditions) Clause(jsToField map[string]string) string {
	sb := strings.Builder{}
	nitem := 0
	for _, sfields := range sc {
		fields := []string{}
		for _, field := range sfields.Fields {
			if dbField, ok := jsToField[field]; ok {
				fields = append(fields, strconv.Quote(dbField))
			}
		}
		if len(fields) == 0 {
			continue
		}

		// separate condition with ,
		if nitem > 0 {
			sb.WriteByte(bComma)
		}

		sb.WriteByte(bLParenthesis)
		sb.WriteString(fields[0])
		for i := 1; i < len(fields); i++ {
			sb.WriteByte(bComma)
			sb.WriteString(fields[i])
		}
		sb.WriteString(") ")
		sb.WriteString(sfields.OrderString())
		nitem++
	}
	return sb.String()
}

func (p *Pagination) Calculate(maxPerPage int64) {
	// get valid per-page count
	perPage := p.PerPage
	if p.PerPage <= 0 {
		if maxPerPage > 0 {
			perPage = maxPerPage
		} else {
			perPage = MaxLimitPerPage
		}
	}

	// if greater than hard limit, set to hard limit
	if perPage > MaxLimitPerPage {
		perPage = MaxLimitPerPage
	}

	p.PerPage = perPage

	if p.Page <= 0 {
		p.Page = 1
	}
	p.offset = (p.Page - 1) * p.PerPage
}

// Offset calculate sql offset (psql)
func (p *Pagination) Offset() int64 {
	return p.offset
}

// Limit return valid number per page
func (p *Pagination) Limit() int64 {
	return p.PerPage
}

// MapQuoteField return quoted DB field
func MapQuoteField(jsToDbMap map[string]string) func(string) (string, error) {
	return func(field string) (string, error) {
		if dbField, ok := jsToDbMap[field]; ok {
			return strconv.Quote(dbField), nil
		}
		return "", errors.New("field " + field + " not found in DB")
	}
}

// QuoteSelectField return quoted DB field
func QuoteSelectField(jsFields []string, jsToDbMap map[string]string) (string, []string) {
	sb := strings.Builder{}
	unknown := []string{}
	nitem := 0
	for _, field := range jsFields {
		if dbField, ok := jsToDbMap[field]; ok {
			if nitem > 0 {
				sb.WriteByte(bComma)
			}
			sb.WriteString(strconv.Quote(dbField))
			nitem++
		} else {
			unknown = append(unknown, field)
		}
	}
	return sb.String(), unknown
}

// JoinSelectColumns for SQL building
func JoinSelectColumns(cols []*SelectColumn) string {
	sb := strings.Builder{}
	nitem := 0
	for _, col := range cols {
		if nitem > 0 {
			sb.WriteByte(bComma)
		}
		sb.WriteString(col.SelectField)
	}
	return sb.String()
}

// FindResultField in select column array
func FindResultField(cols []*SelectColumn, resField string) *SelectColumn {
	for _, col := range cols {
		if strings.EqualFold(resField, col.ResultField) {
			return col
		}
	}
	return nil
}
