package squery

import (
	"io"
	"strconv"
	"strings"
)

// F type for sql field
type F string

// R type for Raw query
type R string

// S type for string
type S string

// M mysql escaper
type M string

// Stringer interface for given value
type Stringer interface {
	String() string
}

type StringBuilder interface {
	Stringer
	io.Writer
	Len() int
	WriteByte(c byte) error
	WriteRune(r rune) (int, error)
	WriteString(s string) (int, error)
}

// simply cast to string
func (s S) String() string {
	return string(s)
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
func (r R) Build(sb StringBuilder, ph Placeholder) ([]interface{}, error) {
	s := string(r)
	if s != "" {
		sb.WriteByte(bLParenthesis)
		sb.WriteString(s)
		sb.WriteByte(bRParenthesis)
	}
	return nil, nil
}
func (r R) IsEmpty() bool {
	return string(r) == ""
}

func (m M) String() string {
	s := string(m)
	if len(s) > 0 {
		// TODO: if already escaped?
		return "`" + s + "`"
	}
	return s
}

// SSliceFrom converts string slice to S
func SSliceFrom(strSlice []string) []Stringer {
	n := len(strSlice)
	sl := make([]Stringer, n)

	for i := 0; i < n; i++ {
		sl[i] = S(strSlice[i])
	}
	return sl
}
