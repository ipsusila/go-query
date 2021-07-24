package squery

import (
	"strings"

	"github.com/jmoiron/sqlx"
)

// ------------------------------------------------------------------------------------------------

// Terminology
// 1. Expression, contains: TERM {OPERATOR} ARG <- for binary expression

type Builder interface {
	Build(sb StringBuilder, ph Placeholder) ([]interface{}, error)
}

// Term in expression
type Term interface {
	Stringer
}
type Raw interface {
	Builder
}

// Expression builder
type Expression interface {
	Builder
	IsEmpty() bool
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
	ILike(term Term, arg interface{}) Expression
	SimilarTo(term Term, arg interface{}) Expression
	NotLike(term Term, arg interface{}) Expression
	NotILike(term Term, arg interface{}) Expression
	NotSimilarTo(term Term, arg interface{}) Expression
	Between(term Term, arg1, arg2 interface{}) Expression
	In(term Term, args ...interface{}) Expression
	NotIn(term Term, args ...interface{}) Expression
	Raw(query string, args ...interface{}) Expression
}

// ------------------------------------------------------------------------------------------------
var Expr ExpressionBuilder = exprBuilder{}

func NewExpressionBuilder() ExpressionBuilder {
	return exprBuilder{}
}

// e.g. name IS NULL
type postExpr struct {
	term Term
	op   string
}

// e.g. NOT (name = 'putu')
type notExpr struct {
	expr Expression
}

// e.g. name = 'putu
type binaryExpr struct {
	term Term
	op   string
	arg  interface{}
}

// e.g. age BETWEEN 30 AND 40
type ternaryExpr struct {
	term Term
	op1  string
	op2  string
	arg1 interface{}
	arg2 interface{}
}

// e.g. age IN (30, 40, 50)
type arrExpr struct {
	term Term
	op   string
	args []interface{}
}

// e.g. (expr) OR (expr) AND (expr)
type arrArgExpr struct {
	op       string
	exprList []Expression
}

// e.g. name in (select name from account where is_active = true)
type rawExpr struct {
	query string
	args  []interface{}
}

// Post, e.g. <PARAM> IS NULL
func (e postExpr) Build(sb StringBuilder, ph Placeholder) ([]interface{}, error) {
	if e.IsEmpty() {
		return nil, nil
	}
	sb.WriteByte(bLParenthesis)
	sb.WriteString(e.term.String())
	sb.WriteByte(bSpace)
	sb.WriteString(e.op)
	sb.WriteByte(bRParenthesis)

	return nil, nil
}
func (e postExpr) IsEmpty() bool {
	return e.term == nil || e.term.String() == ""
}
func (e notExpr) Build(sb StringBuilder, ph Placeholder) ([]interface{}, error) {
	if e.IsEmpty() {
		return nil, nil
	}
	sb.WriteByte(bLParenthesis)
	sb.WriteString(sqlNot)
	sb.WriteByte(bSpace)
	args, err := e.expr.Build(sb, ph)
	sb.WriteByte(bRParenthesis)

	return args, err
}
func (e notExpr) IsEmpty() bool {
	return e.expr == nil || e.expr.IsEmpty()
}

func (e binaryExpr) Build(sb StringBuilder, ph Placeholder) ([]interface{}, error) {
	if e.IsEmpty() {
		return nil, nil
	}
	sb.WriteByte(bLParenthesis)
	sb.WriteString(e.term.String())
	sb.WriteByte(bSpace)
	sb.WriteString(e.op)
	sb.WriteByte(bSpace)
	sb.WriteString(ph.Next())
	sb.WriteByte(bRParenthesis)

	return []interface{}{e.arg}, nil
}
func (e binaryExpr) IsEmpty() bool {
	return e.term == nil || e.term.String() == ""
}

// Expression form: TERM op1 ... op2 ...
// Example: created_at BETWEEN $1 AND $2
//          name ? ... : ...
func (e ternaryExpr) Build(sb StringBuilder, ph Placeholder) ([]interface{}, error) {
	if e.IsEmpty() {
		return nil, nil
	}
	sb.WriteByte(bLParenthesis)
	sb.WriteString(e.term.String())
	sb.WriteByte(bSpace)
	sb.WriteString(e.op1)
	sb.WriteByte(bSpace)
	sb.WriteString(ph.Next())
	sb.WriteByte(bSpace)
	sb.WriteString(e.op2)
	sb.WriteByte(bSpace)
	sb.WriteString(ph.Next())
	sb.WriteByte(bRParenthesis)

	return []interface{}{e.arg1, e.arg2}, nil
}
func (e ternaryExpr) IsEmpty() bool {
	return e.term == nil || e.term.String() == ""
}

// Build expression
func (e arrExpr) Build(sb StringBuilder, ph Placeholder) ([]interface{}, error) {
	if e.IsEmpty() {
		return nil, nil
	}
	sb.WriteByte(bLParenthesis)
	sb.WriteString(e.term.String())
	sb.WriteByte(bSpace)
	sb.WriteString(e.op)
	sb.WriteByte(bSpace)
	sb.WriteString(ph.Next())
	for i := 1; i < len(e.args); i++ {
		sb.WriteByte(bComma)
		sb.WriteString(ph.Next())
	}
	sb.WriteByte(bRParenthesis)

	return e.args, nil
}
func (e arrExpr) IsEmpty() bool {
	return e.term == nil || len(e.args) == 0 || e.term.String() == ""
}

func (e arrArgExpr) IsEmpty() bool {
	return e.op == "" || len(e.exprList) == 0
}

func (e arrArgExpr) Build(sb StringBuilder, ph Placeholder) ([]interface{}, error) {
	var args []interface{}
	nexpr := len(e.exprList)
	switch nexpr {
	case 0:
		return nil, nil
	case 1:
		return e.exprList[0].Build(sb, ph)
	}

	// more than one expression
	sb.WriteByte(bLParenthesis)
	for idx, expr := range e.exprList {
		if expr.IsEmpty() {
			continue
		}
		if idx > 0 {
			sb.WriteByte(bSpace)
			sb.WriteString(e.op)
			sb.WriteByte(bSpace)
		}
		varg, err := expr.Build(sb, ph)
		if err != nil {
			return nil, err
		}
		args = append(args, varg...)
	}
	sb.WriteByte(bRParenthesis)

	return args, nil
}
func (r rawExpr) Build(sb StringBuilder, ph Placeholder) ([]interface{}, error) {
	// Expand ? in case the args is an array
	query, args, err := sqlx.In(r.query, r.args...)
	if err != nil {
		return nil, err
	}

	if r.query != "" {
		sb.WriteByte(bLParenthesis)
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
		sb.WriteByte(bRParenthesis)
	}

	return args, nil
}
func (r rawExpr) IsEmpty() bool {
	return r.query == ""
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
func (e exprBuilder) ILike(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlILike, arg: arg}
}
func (e exprBuilder) SimilarTo(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlSimilarTo, arg: arg}
}
func (e exprBuilder) NotLike(term Term, arg interface{}) Expression {
	return binaryExpr{term: term, op: sqlNotLike, arg: arg}
}
func (e exprBuilder) NotILike(term Term, arg interface{}) Expression {
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
