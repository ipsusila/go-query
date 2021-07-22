package query

type Functor interface {
	Builder
	Func(name string, args ...interface{}) Functor
	Name() string
	Fields() []F
}
