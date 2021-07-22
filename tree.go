package query

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

// ensure expression tree implement expression interface
var _ Expression = (*Tree)(nil)

// constant value
const (
	leftBrace        = '{'
	leftBracket      = '['
	doubleQuote      = '"'
	leftParenthesis  = '('
	rightParenthesis = ')'
	chAnd            = '$'
)

// default key
const (
	defFilterKey = "filter"
)

// termMap map between field and value (either json/simple data)
type termMap map[string]json.RawMessage

// Tree stores tree structure of expression
type Tree struct {
	key  string
	root *treeNode
	data []byte
	fm   FnMapField
	expr *SqlExpression
}

// NexExpressionTree create filter tree representation from JSON with default key, i.e. "filter"
func NexExpressionTree(data []byte) (*Tree, error) {
	return NewExpressionTreeWithKey(data, defFilterKey)
}

// NewExpressionTreeWithKey create filter tree representation from JSON for given filter key
func NewExpressionTreeWithKey(data []byte, key string) (*Tree, error) {
	tree := &Tree{
		data: data,
		key:  key,
	}
	if err := tree.parse(); err != nil {
		return nil, err
	}

	return tree, nil
}

// Option set generator option
func (t *Tree) FieldMapper(fm FnMapField) *Tree {
	t.fm = fm
	return t
}

// Build implement builder interface
func (t *Tree) Build(sb *strings.Builder, ph Placeholder) ([]interface{}, error) {
	if t.expr != nil {
		// if already parsed, return it
		return t.expr.Args, nil
	}

	// parse if the tree is not empty
	if t.root != nil {
		expr, err := t.root.build(sb, ph, t.fm)
		if err != nil {
			return nil, err
		}
		t.expr = expr
	}
	return nil, nil
}

// SqlExpression return sql expression.
// If this method is called before Build, it will return NULL.
func (t *Tree) SqlExpression() *SqlExpression {
	return t.expr
}

// parse JSON data to tree
func (t *Tree) parse() error {
	// clean up data
	i := 0
	nd := len(t.data)
	for i < nd {
		switch t.data[i] {
		case '\t', '\r', '\n', '\v', ' ':
			i++
		default:
			goto clean
		}
	}
clean:
	rootNode := treeNode{
		Term:   t.key,
		Data:   t.data[i:],
		isRoot: true,
	}
	err := t.parseToNode(&rootNode)
	if err != nil {
		return err
	}
	t.root = &rootNode
	return nil
}

func (t *Tree) parseToNode(nd *treeNode) error {
	if len(nd.Data) == 0 {
		return errors.New("empty value")
	}

	// create operator
	switch nd.Data[0] {
	case leftBrace:
		// object, AND operation
		tmAnd := termMap{}
		if err := json.Unmarshal(nd.Data, &tmAnd); err != nil {
			return err
		}

		// loop through term
		nd.Value = opAnd
		nd.ValueType = tOperator
		for key, val := range tmAnd {
			term := strings.TrimSpace(key)
			if term == "" {
				continue
			}
			childNode := treeNode{
				Term: term,
				Data: []byte(val),
			}
			nd.Children = append(nd.Children, &childNode)
			if err := t.parseToNode(&childNode); err != nil {
				return err
			}
		}
	case leftBracket:
		switch nd.Term {
		case opBetween:
			var arr []interface{}
			if err := json.Unmarshal(nd.Data, &arr); err != nil {
				return err
			}
			if len(arr) != 2 {
				return errors.New("$between operator needs array args with 2 values")
			}
			nd.Value = arr
			nd.ValueType = tArrayBetween
			return nil
		case opIn, opNotIn:
			var arr []interface{}
			if err := json.Unmarshal(nd.Data, &arr); err != nil {
				return err
			}
			nd.Value = arr
			nd.ValueType = tArray
			return nil
		default:
			// array, OR operation
			// object, AND operation
			tmOr := []termMap{}
			if err := json.Unmarshal(nd.Data, &tmOr); err != nil {
				return err
			}

			// loop through term
			nd.Value = opOr
			nd.ValueType = tOperator
			for _, item := range tmOr {
				for key, val := range item {
					term := strings.TrimSpace(key)
					if term == "" {
						continue
					}
					childNode := treeNode{
						Term: term,
						Data: []byte(val),
					}
					nd.Children = append(nd.Children, &childNode)
					if err := t.parseToNode(&childNode); err != nil {
						return err
					}
				}
			}
		}

	case doubleQuote:
		// string data
		str, err := strconv.Unquote(string(nd.Data))
		if err != nil {
			return err
		}
		nd.Value = str
		nd.ValueType = tString
		return nil
	default:
		data := nd.Data
		if bytes.Equal(vNull, data) {
			nd.Value = nil
			nd.ValueType = tNull
		} else if bytes.Equal(vTrue, data) {
			nd.Value = true
			nd.ValueType = tBoolean
		} else if bytes.Equal(vFalse, data) {
			nd.Value = false
			nd.ValueType = tBoolean
		} else {
			// perhaps a number
			fv, err := strconv.ParseFloat(string(data), 64)
			if err != nil {
				return err
			}
			nd.Value = fv
			nd.ValueType = tNumber
		}
		return nil
	}
	return nil
}
