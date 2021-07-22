package query

import (
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

// ------------------------------------------------------------------------------------------------

// Terminology
// 1. Expression, contains: TERM {OPERATOR} ARG <- for binary expression

type mysqlEscaper struct {
	val string
}

func (m mysqlEscaper) String() string {
	if len(m.val) > 0 {
		// TODO: if already escaped?
		return "`" + m.val + "`"
	}
	return m.val
}

// Abbreviation for field (sql field)
type F string
type R string

func M(s string) Stringer {
	return mysqlEscaper{s}
}

// Stringer interface for given value
type Stringer interface {
	String() string
}

type StringAccumulator interface {
	Stringer
	WriteRune(r rune) (int, error)
	WriteString(s string) (int, error)
}

type Builder interface {
	Build(sb *strings.Builder, ph Placeholder) ([]interface{}, error)
}

// Term in expression
type Term interface {
	Stringer
}

type Parameter interface {
	Stringer
}

// Operator of given expression
type Operator interface {
	Stringer
}

type Raw interface {
	Builder
}

// Expression builder
type Expression interface {
	Builder
}

// ExpressionBuilder
type ExpressionBuilder interface {
	Or(expr1, expr2 Expression, exprs ...Expression) Expression
	And(expr1, expr2 Expression, exprs ...Expression) Expression
	Null(term Term) Expression
	NotNull(term Term) Expression
	Not(expr Expression) Expression
	Eq(term Term, arg interface{}) Expression
	Neq(term Term, arg interface{}) Expression
	Gt(term Term, arg interface{}) Expression
	Gte(term Term, arg interface{}) Expression
	Lt(term Term, arg interface{}) Expression
	Lte(term Term, arg interface{}) Expression
	Like(term Term, arg interface{}) Expression
	Ilike(term Term, arg interface{}) Expression
	SimilarTo(term Term, arg interface{}) Expression
	NotLike(term Term, arg interface{}) Expression
	NotIlike(term Term, arg interface{}) Expression
	NotSimilarTo(term Term, arg interface{}) Expression
	Between(term Term, arg1, arg2 interface{}) Expression
	In(term Term, args ...interface{}) Expression
	NotIn(term Term, args ...interface{}) Expression
	Raw(query string, args ...interface{}) Expression
}

// String representation of the field
func (f F) String() string {
	items := strings.Split(string(f), ".")
	for i := 0; i < len(items); i++ {
		items[i] = strconv.Quote(items[i])
	}
	return strings.Join(items, ".")
}

func (r R) String() string {
	return string(r)
}
func (r R) Build(sb *strings.Builder, ph Placeholder) ([]interface{}, error) {
	s := string(r)
	if s != "" {
		sb.WriteRune('(')
		sb.WriteString(s)
		sb.WriteRune(')')
	}
	return nil, nil
}

// ------------------------------------------------------------------------------------------------
var Expr ExpressionBuilder = exprBuilder{}

func NewExpressionBuilder() ExpressionBuilder {
	return exprBuilder{}
}

type postExpr struct {
	term Term
	op   string
}
type notExpr struct {
	expr Expression
}
type binaryExpr struct {
	term Term
	op   string
	arg  interface{}
}
type ternaryExpr struct {
	term Term
	op1  string
	op2  string
	arg1 interface{}
	arg2 interface{}
}

type arrExpr struct {
	term Term
	op   string
	args []interface{}
}

type arrArgExpr struct {
	op       string
	exprList []Expression
}
type rawExpr struct {
	query string
	args  []interface{}
}

// Post, e.g. <PARAM> IS NULL
func (e postExpr) Build(sb *strings.Builder, ph Placeholder) ([]interface{}, error) {
	if e.term == nil {
		return nil, nil
	}
	sb.WriteRune('(')
	sb.WriteString(e.term.String())
	sb.WriteRune(' ')
	sb.WriteString(e.op)
	sb.WriteRune(')')

	return nil, nil
}
func (e notExpr) Build(sb *strings.Builder, ph Placeholder) ([]interface{}, error) {
	if e.expr == nil {
		return nil, nil
	}
	sb.WriteRune('(')
	sb.WriteString(sqlNot)
	sb.WriteRune(' ')
	args, err := e.expr.Build(sb, ph)
	sb.WriteRune(')')

	return args, err
}

func (e binaryExpr) Build(sb *strings.Builder, ph Placeholder) ([]interface{}, error) {
	if e.term == nil {
		return nil, nil
	}
	sb.WriteRune('(')
	sb.WriteString(e.term.String())
	sb.WriteRune(' ')
	sb.WriteString(e.op)
	sb.WriteRune(' ')
	sb.WriteString(ph.Next())
	sb.WriteRune(')')

	return []interface{}{e.arg}, nil
}

// Expression form: TERM op1 ... op2 ...
// Example: created_at BETWEEN $1 AND $2
//          name ? ... : ...
func (e ternaryExpr) Build(sb *strings.Builder, ph Placeholder) ([]interface{}, error) {
	if e.term == nil {
		return nil, nil
	}
	sb.WriteRune('(')
	sb.WriteString(e.term.String())
	sb.WriteRune(' ')
	sb.WriteString(e.op1)
	sb.WriteRune(' ')
	sb.WriteString(ph.Next())
	sb.WriteRune(' ')
	sb.WriteString(e.op2)
	sb.WriteRune(' ')
	sb.WriteString(ph.Next())
	sb.WriteRune(')')

	return []interface{}{e.arg1, e.arg2}, nil
}

// Build expression
func (e arrExpr) Build(sb *strings.Builder, ph Placeholder) ([]interface{}, error) {
	if e.term == nil || len(e.args) == 0 {
		return nil, nil
	}
	sb.WriteRune('(')
	sb.WriteString(e.term.String())
	sb.WriteRune(' ')
	sb.WriteString(e.op)
	sb.WriteRune(' ')
	sb.WriteString(ph.Next())
	for i := 1; i < len(e.args); i++ {
		sb.WriteRune(',')
		sb.WriteString(ph.Next())
	}
	sb.WriteRune(')')

	return e.args, nil
}

func (e arrArgExpr) Build(sb *strings.Builder, ph Placeholder) ([]interface{}, error) {
	var args []interface{}
	nexpr := len(e.exprList)
	switch nexpr {
	case 0:
		return nil, nil
	case 1:
		return e.exprList[0].Build(sb, ph)
	}

	// more than one expression
	sb.WriteRune('(')
	for idx, expr := range e.exprList {
		if idx > 0 {
			sb.WriteRune(' ')
			sb.WriteString(e.op)
			sb.WriteRune(' ')
		}
		varg, err := expr.Build(sb, ph)
		if err != nil {
			return nil, err
		}
		args = append(args, varg...)
	}
	sb.WriteRune(')')

	return args, nil
}
func (r rawExpr) Build(sb *strings.Builder, ph Placeholder) ([]interface{}, error) {
	// Expand ? in case the args is an array
	query, args, err := sqlx.In(r.query, r.args...)
	if err != nil {
		return nil, err
	}

	if r.query != "" {
		sb.WriteRune('(')
	}

	// replace placeholder with number
	offset := 0
	for offset = strings.IndexRune(query, '?'); offset != -1; offset = strings.IndexRune(query, '?') {
		sb.WriteString(query[:offset])
		sb.WriteString(ph.Next())
		query = query[offset+1:]
	}
	sb.WriteString(query)
	if r.query != "" {
		sb.WriteRune(')')
	}

	return args, nil
}

type exprBuilder struct{}

func (e exprBuilder) Or(expr1, expr2 Expression, exprs ...Expression) Expression {
	orExpr := arrArgExpr{
		op:       sqlOr,
		exprList: []Expression{expr1, expr2},
	}
	orExpr.exprList = append(orExpr.exprList, exprs...)

	return orExpr
}

func (e exprBuilder) And(expr1, expr2 Expression, exprs ...Expression) Expression {
	orExpr := arrArgExpr{
		op:       sqlAnd,
		exprList: []Expression{expr1, expr2},
	}
	orExpr.exprList = append(orExpr.exprList, exprs...)

	return orExpr
}

func (e exprBuilder) Null(term Term) Expression {
	return postExpr{term: term, op: sqlIsNull}
}
func (e exprBuilder) NotNull(term Term) Expression {
	return postExpr{term: term, op: sqlIsNotNull}
}
func (e exprBuilder) Not(expr Expression) Expression {
	return notExpr{expr: expr}
}
func (e exprBuilder) Eq(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlEq, arg: arg}
}
func (e exprBuilder) Neq(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlNeq, arg: arg}
}
func (e exprBuilder) Gt(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlGt, arg: arg}
}
func (e exprBuilder) Gte(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlGte, arg: arg}
}
func (e exprBuilder) Lt(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlLt, arg: arg}
}
func (e exprBuilder) Lte(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlLte, arg: arg}
}
func (e exprBuilder) Like(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlLike, arg: arg}
}
func (e exprBuilder) Ilike(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlILike, arg: arg}
}
func (e exprBuilder) SimilarTo(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlSimilarTo, arg: arg}
}
func (e exprBuilder) NotLike(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlNotLike, arg: arg}
}
func (e exprBuilder) NotIlike(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlNotILike, arg: arg}
}
func (e exprBuilder) NotSimilarTo(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlNotSimilarTo, arg: arg}
}
func (e exprBuilder) Between(term Term, arg1, arg2 interface{}) Expression {
	return ternaryExpr{term: term, arg1: arg1, arg2: arg2, op1: sqlBetween, op2: sqlAnd}
}

func (e exprBuilder) In(term Term, args ...interface{}) Expression {
	return arrExpr{term: term, op: sqlIn, args: args}
}

func (e exprBuilder) NotIn(term Term, args ...interface{}) Expression {
	return arrExpr{term: term, op: sqlNotIn, args: args}
}

func (e exprBuilder) Raw(query string, args ...interface{}) Expression {
	return rawExpr{query: query, args: args}
}
