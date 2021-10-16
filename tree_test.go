package squery_test

import (
	"strings"
	"testing"

	"github.com/ipsusila/go-util"
	qy "github.com/ipsusila/squery"
	"github.com/stretchr/testify/assert"
)

const jsData = `{
	"status": "A",
	"$or": [{
			"qty": {
				"$lt": 30
			}
		},
		{
			"item": "/^p/"
		}
	],
	"name": null,
	"locked": false,
	" ": {}
}`

const jsData2 = `{"name": {"$like": "%KALI%"}}`

const jsData3 = `
{
	"$or": [
		{"field": 10},
		{"item": {"$gt": 300}},
		{"value": {"$in": [100, 200]}},
		{"age": {"$between": [20, 30]}},
		{"empty": ""}
	]
}
`

const jsData4 = `
{
	"$not": {
		"fieldEmpty": ""
	}
}
`

const jsData5 = `
{
	"officialName": {"$like": "%123%"}
}
`

var jsArray = []string{
	jsData,
	jsData2,
	jsData3,
	jsData4,
	jsData5,
}

func parseJson(data []byte) (*qy.Tree, error) {
	fm := func(field string) (string, error) {
		return `"` + field + `"`, nil
	}
	tree, err := qy.NewExpressionTree(data, fm)
	if err != nil {
		return nil, err
	}

	ph := qy.NewQmPlaceholder()
	sb := strings.Builder{}
	_, err = tree.Build(&sb, ph)
	if err != nil {
		return nil, err
	}
	return tree, nil
}

func testParser(data []byte, t *testing.T) {
	tree, err := parseJson(data)
	assert.NoError(t, err, "Parsing JSON should not failed/error")
	where := tree.SqlExpression()
	t.Logf("<<SQL>>\n%s\n", util.PrettyColorStr(where))
	t.Logf("<<CLAUSE>>: %s\n", where.Clause)
	t.Logf("<<ARGS>>%v\n", where.Args)
	t.Logf("<<FIELDS>>%v\n", where.Fields)
	t.Logf("<<SQLFIELDS>>%s\n", where.SqlFields)
}

func TestParser(t *testing.T) {
	for _, str := range jsArray {
		testParser([]byte(str), t)
	}
	testParser(nil, t)
}

func BenchmarkTreeparser0(t *testing.B) {
	for n := 0; n < t.N; n++ {
		parseJson([]byte(jsArray[0]))
	}
}

func BenchmarkTreeparser1(t *testing.B) {
	for n := 0; n < t.N; n++ {
		parseJson([]byte(jsArray[1]))
	}
}

func BenchmarkTreeparser2(t *testing.B) {
	for n := 0; n < t.N; n++ {
		parseJson([]byte(jsArray[2]))
	}
}

func BenchmarkTreeparser3(t *testing.B) {
	for n := 0; n < t.N; n++ {
		parseJson([]byte(jsArray[3]))
	}
}
