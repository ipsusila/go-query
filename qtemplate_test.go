package squery_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	qy "github.com/ipsusila/squery"
)

func buildQuery(b *testing.B, i int) {
	tpl := `
		SELECT DISTINCT {{COLUMNS}} FROM country AS tc
		LEFT JOIN district AS td ON tc.id=td.id
		{{WHERE}} AND {{idField}} IN {{idField_value}} 
		{{GROUPBY}} {{ORDERBY}} {{LIMIT}}
	`
	cntTpl := "SELECT COUNT(*) FROM country {{WHERE}}"

	const filter = `
	{
		"$or": [
			{"field": 10},
			{"item": {"$gt": 300}},
			{"itemValue": {"$in": [100, 200]}},
			{"age": {"$between": [20, 30]}},
			{"empty": ""}
		]
	}`

	listArg := qy.TemplateListSearchArg{
		ListSearchArg: qy.ListSearchArg{
			Filter: json.RawMessage(filter),
			Pagination: &qy.Pagination{
				Page:    1,
				PerPage: 40,
			},
			Fields: []string{"age", "itemValue", "item"},
		},
		SelectTemplate: tpl,
		CountTemplate:  cntTpl,
		FieldValues: map[string]interface{}{
			"idField": []string{"one", "two", "", "%"},
		},
		FieldsMap: map[string]string{
			"age":  "data->>'age'",
			"item": "custom_item",
		},
		SelectColsMap: map[string]string{
			"age": "data->>'age' AS age",
		},
	}

	tree, err := qy.NewExpressionTree([]byte(filter), listArg.FieldMapper)
	if err != nil {
		b.Fatal(err)
	}

	ph := qy.NewPsqlPlaceholder()
	sb := strings.Builder{}

	qTpl := qy.NewTemplateQuery(tpl, cntTpl, listArg.FieldMapper, listArg.FieldValues)
	qTpl.Columns(listArg.FieldsToColumns()...).
		Where(tree).
		Limit(10).
		Offset(3)

	_, err = qTpl.Build(&sb, ph)
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkTempalteQuery(b *testing.B) {
	start := time.Now()
	for i := 0; i < b.N; i++ {
		buildQuery(b, i)
	}
	dur := time.Since(start) / time.Duration(b.N)
	b.Logf("Iteration: %d, duration: %v", b.N, dur)
}

func TestTemplateQuery(t *testing.T) {
	tpl := `
		SELECT DISTINCT {{COLUMNS}} FROM country AS tc
		LEFT JOIN district AS td ON tc.id=td.id
		{{WHERE}} AND {{idField}} IN {{idField_value}} 
		{{GROUPBY}} {{ORDERBY}} {{LIMIT}} {{OFFSET}}
	`
	cntTpl := "SELECT COUNT(*) FROM country {{WHERE}}"

	const filter = `
	{
		"$or": [
			{"field": 10},
			{"item": {"$gt": 300}},
			{"value": {"$in": [100, 200]}},
			{"age": {"$between": [20, 30]}},
			{"empty": ""}
		]
	}`

	listArg := qy.TemplateListSearchArg{
		ListSearchArg: qy.ListSearchArg{
			Filter: json.RawMessage(filter),
			Pagination: &qy.Pagination{
				Page:    1,
				PerPage: 40,
			},
			Fields: []string{"age", "itemValue", "item"},
		},
		SelectTemplate: tpl,
		CountTemplate:  cntTpl,
		FieldValues: map[string]interface{}{
			"idField": []string{"one", "two", "", "%"},
		},
		FieldsMap: map[string]string{
			"age":  "data->>'age'",
			"item": "custom_item",
		},
		SelectColsMap: map[string]string{
			"age": "data->>'age' AS age",
		},
	}

	tree, err := qy.NewExpressionTree([]byte(filter), listArg.FieldMapper)
	if err != nil {
		t.Fatal(err)
	}

	ph := qy.NewPsqlPlaceholder()
	sb := strings.Builder{}

	qTpl := qy.NewTemplateQuery(tpl, cntTpl, listArg.FieldMapper, listArg.FieldValues)
	qTpl.Columns(listArg.FieldsToColumns()...).
		Where(tree).
		Limit(10).
		Offset(3)

	args, err := qTpl.Build(&sb, ph)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("BUILD:", sb.String())
	t.Log("ARGS:", args)

	// check select
	strSelect, args, err := qTpl.Select()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("SELECT:", strSelect)
	t.Logf("ARGS: %#v", args)

	strCount, args, err := qTpl.Count()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("COUNT:", strCount)
	t.Log("ARGS:", args)
}
