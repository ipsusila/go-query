package query

import "strings"

type Expressions interface {
	Builder
	Expression() Expression
	Set(expr Expression) Expressions
	Or(expr Expression, exprs ...Expression) Expressions
	And(expr Expression, exprs ...Expression) Expressions
}

type chainableExpression struct {
	expr Expression
}

// FE for fluent expression constructor
var FE Expressions = &chainableExpression{}

// NewExpressions construct new fluent expression
func NewExpressions() Expressions {
	return &chainableExpression{}
}

func (c *chainableExpression) Build(sb *strings.Builder, ph Placeholder) ([]interface{}, error) {
	if c.expr != nil {
		return c.expr.Build(sb, ph)
	}
	// empty expression
	return nil, nil
}

// Expression return current expression
func (c *chainableExpression) Expression() Expression {
	return c.expr
}

// Set overwrites current expression with expr
func (c *chainableExpression) Set(expr Expression) Expressions {
	c.expr = expr
	return c
}

// Or add OR operation to expression list
func (c *chainableExpression) Or(expr Expression, exprs ...Expression) Expressions {
	if c.expr == nil {
		if len(exprs) == 0 {
			c.expr = arrArgExpr{
				op:       sqlOr,
				exprList: []Expression{expr},
			}
		} else {
			c.expr = Expr.Or(c.expr, expr, exprs...)
		}
	} else {
		c.expr = Expr.Or(c.expr, expr, exprs...)
	}
	return c
}

// And adds AND operation to expression list
func (c *chainableExpression) And(expr Expression, exprs ...Expression) Expressions {
	if c.expr == nil {
		if len(exprs) == 0 {
			c.expr = arrArgExpr{
				op:       sqlAnd,
				exprList: []Expression{expr},
			}
		} else {
			c.expr = Expr.And(c.expr, expr, exprs...)
		}
	} else {
		c.expr = Expr.And(c.expr, expr, exprs...)
	}
	return c
}
