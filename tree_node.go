package squery

import (
	"errors"
)

// stores treenode
type treeNode struct {
	Term      string      `json:"term"`
	Data      []byte      `json:"-"`
	Value     interface{} `json:"value"`
	ValueType valueType   `json:"valueType"`
	Children  []*treeNode `json:"children"`

	isRoot bool
}

func (fn *treeNode) build(sb StringBuilder, ph Placeholder, fm FnMapField) (*SqlExpression, error) {
	whereArg := SqlExpression{
		ph: ph,
		fm: fm,
	}
	err := fn.traverseNode(sb, &whereArg)
	if err != nil {
		return nil, err
	}

	return &whereArg, nil
}

func (fn *treeNode) operatorString(op string) bool {
	if len(op) == 0 {
		return false
	}
	return op[0] == chAnd
}

// isOperator return true if term start with '$'
func (fn *treeNode) isOperator() bool {
	return fn.operatorString(fn.Term)
}

func (fn *treeNode) isOr() bool {
	return fn.Term == opOr
}

func (fn *treeNode) traverseNode(sb StringBuilder, arg *SqlExpression) error {
	// number of children
	nchildren := len(fn.Children)
	switch nchildren {
	case 0:
		sb.WriteByte(bLParenthesis)
		fn.writeLeaf(sb, arg)
		sb.WriteByte(bRParenthesis)
	case 1:
		if fn.isRoot {
			fn.Children[0].traverseNode(sb, arg)
		} else {
			sb.WriteByte(bLParenthesis)
			if err := fn.tryWriteTerm(sb, arg); err != nil {
				return err
			}
			if err := fn.tryWriteOperator(sb); err != nil {
				return err
			}
			fn.Children[0].writeLeaf(sb, arg)
			sb.WriteByte(bRParenthesis)
		}
	default:
		sb.WriteByte(bLParenthesis)
		op := opToSQL[opAnd]
		if fn.isOr() {
			op = opToSQL[opOr]
		}

		if err := fn.Children[0].traverseNode(sb, arg); err != nil {
			return err
		}
		for i := 1; i < nchildren; i++ {
			sb.WriteByte(bSpace)
			sb.WriteString(op)
			sb.WriteByte(bSpace)
			if err := fn.Children[i].traverseNode(sb, arg); err != nil {
				return err
			}
		}
		sb.WriteByte(bRParenthesis)
	}

	return nil
}

// writeLeaf node, i.e. node that don't has any children
func (fn *treeNode) writeLeaf(sb StringBuilder, arg *SqlExpression) error {
	if fn.isOperator() {
		if err := fn.tryWriteOperator(sb); err != nil {
			return err
		}
		if err := fn.writeValue(sb, arg); err != nil {
			return err
		}
	} else {
		sqlField, err := arg.mappedField(fn.Term)
		if err != nil {
			return err
		}
		arg.Fields = append(arg.Fields, fn.Term)
		arg.SqlFields = append(arg.SqlFields, sqlField)

		sb.WriteString(sqlField)
		op := opToSQL[opEq]
		if fn.ValueType == tNull {
			op = opToSQL[opIs]
		}
		sb.WriteByte(bSpace)
		sb.WriteString(op)
		sb.WriteByte(bSpace)
		if err := fn.writeValue(sb, arg); err != nil {
			return err
		}
	}
	return nil
}

// try to write operator if term is operator
func (fn *treeNode) tryWriteOperator(sb StringBuilder) error {
	if fn.isOperator() {
		op, ok := opToSQL[fn.Term]
		if !ok {
			return errors.New(fn.Term + ": unknown operator")
		}
		sb.WriteByte(bSpace)
		sb.WriteString(op)
		sb.WriteByte(bSpace)
	}
	return nil
}

// try to write term if the term is not operator
func (fn *treeNode) tryWriteTerm(sb StringBuilder, arg *SqlExpression) error {
	if fn.isRoot {
		return nil
	}
	if !fn.isOperator() {
		sqlField, err := arg.mappedField(fn.Term)
		if err != nil {
			return err
		}
		arg.Fields = append(arg.Fields, fn.Term)
		arg.SqlFields = append(arg.SqlFields, sqlField)

		sb.WriteString(sqlField)
	}
	return nil
}

// write value section of the node
func (fn *treeNode) writeValue(sb StringBuilder, arg *SqlExpression) error {
	switch fn.ValueType {
	case tNull:
		sb.WriteString("NULL")
	case tBoolean, tNumber, tString:
		sb.WriteString(arg.ph.Next())
		arg.Args = append(arg.Args, fn.Value)
	case tArray:
		args, ok := fn.Value.([]interface{})
		if !ok {
			return errors.New("argument is not array")
		}
		sb.WriteByte(bLParenthesis)
		sb.WriteString(arg.ph.Next())
		for i := 1; i < len(args); i++ {
			sb.WriteByte(bComma)
			sb.WriteString(arg.ph.Next())
		}
		sb.WriteByte(bRParenthesis)
		arg.Args = append(arg.Args, args...)
	case tArrayBetween:
		arrArgs, ok := fn.Value.([]interface{})
		if !ok || len(arrArgs) < 2 {
			return errors.New("argument for BETWEEN must be an array with 2 elements")
		}
		sb.WriteString(arg.ph.Next())
		sb.WriteString(" AND ")
		sb.WriteString(arg.ph.Next())
		arg.Args = append(arg.Args, arrArgs[0], arrArgs[1])
	}
	return nil
}
