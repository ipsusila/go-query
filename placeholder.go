package query

import "strconv"

// Placeholder in sql query, e.g. $1, $2, ?
type Placeholder interface {
	Next() string
	Position() int
}

type psqlPlaceholder struct {
	pos     int
	initPos int
}

type qmPlaceholder struct {
	pos int
}

// NewPsqlPlaceholder create Postgresql place holder with $ prefix.
func NewPsqlPlaceholder(initVal ...int) Placeholder {
	pos := 0
	if len(initVal) > 0 {
		pos = initVal[0] - 1
	}
	if pos < 0 {
		panic("Specified position must be positive number")
	}
	return &psqlPlaceholder{
		initPos: pos,
		pos:     pos,
	}
}

// NeqQmPlaceholder return placeholder with question mark
func NewQmPlaceholder() Placeholder {
	return &qmPlaceholder{}
}

func (p *psqlPlaceholder) Next() string {
	p.pos++
	return sqlDollar + strconv.Itoa(p.pos)
}

func (p *psqlPlaceholder) Position() int {
	return p.pos
}

func (p *qmPlaceholder) Next() string {
	p.pos++
	return "?"
}
func (p *qmPlaceholder) Position() int {
	return p.pos
}
