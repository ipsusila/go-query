package query_test

import (
	"strings"
	"testing"
	"time"

	qy "github.com/ipsusila/go-query"
)

func TestQuery(t *testing.T) {
	exp := qy.NewExpressionBuilder()
	expr := exp.And(
		exp.Eq(qy.F("p.name"), "John"),
		exp.Like(qy.F("address"), "%jakarta%"),
		exp.Raw("place IN (?)", []interface{}{"serpong", "bekasi"}),
		exp.Or(
			exp.Gt(qy.F("age"), 10),
			exp.Not(
				exp.Eq(qy.F("salary"), 100000),
			),
		),
		exp.Raw("id in select id from table where name=?", "Travolta"),
		qy.R("LOWER(job)='programmer'"),
		exp.Between(qy.F("created_at"), time.Now(), time.Now()),
	)
	ph := qy.NewPsqlPlaceholder()
	sb := strings.Builder{}
	args, err := expr.Build(&sb, ph)
	t.Logf("Expression: %s, Args: %v, Err: %v\n", sb.String(), args, err)

	qry := qy.NewQuery()
	query, args, err := qry.From(qy.R("table")).Where(expr).Count()
	t.Logf("Query: %s, args: %v, err: %v", query, args, err)

	query, args, err = qry.Select(qy.F("name"), qy.F("address"))
	t.Logf("Query: %s, args: %v, err: %v", query, args, err)
}

func TestFluentExpression(t *testing.T) {
	fe := qy.NewExpressions()
	exp := qy.NewExpressionBuilder()
	b := fe.Set(exp.Eq(qy.F("abc"), 10)).
		And(exp.Gt(qy.F("xyz"), 11)).
		Or(exp.Like(qy.F("text"), "%ABC%"))
	sb := strings.Builder{}
	ph := qy.NewPsqlPlaceholder()
	args, err := b.Build(&sb, ph)

	t.Logf("Expression: %s, args: %v, err: %v", sb.String(), args, err)

}
