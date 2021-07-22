package query

const (
	opAnd           = "$and"
	opOr            = "$or"
	opNot           = "$not"
	opIs            = "$is"
	opIsNot         = "$isnot"
	opEq            = "$eq"
	opNeq           = "$neq"
	opGt            = "$gt"
	opGte           = "$gte"
	opLt            = "$lt"
	opLte           = "$lte"
	opIn            = "$in"
	opNotIn         = "$nin"
	opLike          = "$like"
	opNotLike       = "$nlike"
	opILike         = "$ilike"
	opNotILike      = "$nilike"
	opBetween       = "$between"
	opSimilarTo     = "$similarto"
	opNotSimilarTo  = "$nsimilarto"
	sqlAnd          = "AND"
	sqlOr           = "OR"
	sqlNot          = "NOT"
	sqlIs           = "IS"
	sqlIsNot        = "IS NOT"
	sqlEq           = "="
	sqlNeq          = "<>"
	sqlGt           = ">"
	sqlGte          = ">="
	sqlLt           = "<"
	sqlLte          = "<="
	sqlIn           = "IN"
	sqlNotIn        = "NOT IN"
	sqlLike         = "LIKE"
	sqlNotLike      = "NOT LIKE"
	sqlILike        = "ILIKE"
	sqlNotILike     = "NOT ILIKE"
	sqlBetween      = "BETWEEN"
	sqlSimilarTo    = "SIMILAR TO"
	sqlNotSimilarTo = "NOT SIMILAR TO"
	sqlIsNull       = "IS NULL"
	sqlIsNotNull    = "IS NOT NULL"
	sqlDollar       = "$"
)

// known operator maps
var opToSQL = map[string]string{
	opAnd:          sqlAnd,
	opOr:           sqlOr,
	opNot:          sqlNot,
	opIs:           sqlIs,
	opIsNot:        sqlIsNot,
	opEq:           sqlEq,
	opNeq:          sqlNeq,
	opGt:           sqlGt,
	opGte:          sqlGte,
	opLt:           sqlLt,
	opLte:          sqlLte,
	opIn:           sqlIn,
	opNotIn:        sqlNotIn,
	opLike:         sqlLike,
	opNotLike:      sqlNotLike,
	opILike:        sqlILike,
	opNotILike:     sqlNotILike,
	opBetween:      sqlBetween,
	opSimilarTo:    sqlSimilarTo,
	opNotSimilarTo: sqlNotSimilarTo,
}
