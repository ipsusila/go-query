package squery

import (
	"context"

	logger "github.com/ipsusila/slog"
	"github.com/jmoiron/sqlx"
)

// Aiias to slice of map[string]interface{}
type MapSlice []map[string]interface{}

// FieldMapSelector to convert map field's to other field, e.g. JSON
type FieldMapSelector func(field string, val interface{}) (string, interface{}, bool)

type Error interface {
	Err() error
}

// Querier execute query
type Querier interface {
	Error
	One(ctx context.Context, dest interface{}) error
	OneMap(ctx context.Context, fm FieldMapSelector) (map[string]interface{}, error)
	Many(ctx context.Context, dest interface{}) error
	ManyMap(ctx context.Context, fm FieldMapSelector) (MapSlice, error)
	Count(ctx context.Context) (int64, error)
}

// QuerierConstructor is interface for building various querier
type QuerierConstructor interface {
	NamedQuery(query string, arg interface{}, hasIn bool, logFields ...interface{}) Querier
	Query(query string, args []interface{}, logFields ...interface{}) Querier
	InQuery(query string, args []interface{}, logFields ...interface{}) Querier
	RebindQuery(query string, args []interface{}, logFields ...interface{}) Querier
	WithSelector(s Selector, logFields ...interface{}) Querier
}

// sql string querier
type querier struct {
	c         *querierConstructor
	logFields []interface{}
	query     string
	args      []interface{}
	err       error
}

// select selector querier
type sbQuerier struct {
	*querier
	selector Selector
}

type querierConstructor struct {
	db  *sqlx.DB
	log logger.Logger
}

func NewQuerierConstructor(db *sqlx.DB, log logger.Logger) QuerierConstructor {
	return &querierConstructor{db: db, log: log}
}

// NamedQuery assign any query with named place holder e.g. id = :id, city = :city to querier.
// It will call Bind to replace the placeholder into db specific placeholder.
func (c *querierConstructor) NamedQuery(query string, arg interface{}, hasIn bool, logFields ...interface{}) Querier {
	q := querier{c: c, query: query, logFields: logFields}
	if c.log.HasLevel(logger.TraceLevel) {
		c.log.Tracew("construct `NamedQuery`",
			append(logFields,
				"query", query,
				"arg", arg,
				"hasIn", hasIn)...)
	}
	if hasIn {
		query, args, err := sqlx.Named(query, arg)
		if q.err = err; q.err == nil {
			query, args, err := sqlx.In(query, args...)
			if q.err = err; q.err == nil {
				query = c.db.Rebind(query)
			}
			q.query = query
			q.args = args
		}
	} else {
		q.query, q.args, q.err = c.db.BindNamed(query, arg)
	}
	return &q
}

// Query assign sql query that already has place holder specific to the DB driver
func (c *querierConstructor) Query(query string, args []interface{}, logFields ...interface{}) Querier {
	if c.log.HasLevel(logger.TraceLevel) {
		c.log.Tracew("construct `Query`",
			append(logFields,
				"query", query,
				"args", args)...)
	}
	return &querier{c: c, query: query, args: args, logFields: logFields}
}

// InQuery assign a query that has IN/NOT IN statetements,
func (c *querierConstructor) InQuery(query string, args []interface{}, logFields ...interface{}) Querier {
	if c.log.HasLevel(logger.TraceLevel) {
		c.log.Tracew("construct `InQuery`",
			append(logFields,
				"query", query,
				"args", args)...)
	}
	q := &querier{c: c, query: query, args: args, logFields: logFields}
	if query, q.args, q.err = sqlx.In(query, args...); q.err == nil {
		q.query = q.c.db.Rebind(query)
	}
	return q
}

// RebindQuery replace ? place holder to place holder of the DB
func (c *querierConstructor) RebindQuery(query string, args []interface{}, logFields ...interface{}) Querier {
	if c.log.HasLevel(logger.TraceLevel) {
		c.log.Tracew("construct `RebindQuery`",
			append(logFields,
				"query", query,
				"args", args)...)
	}
	q := &querier{c: c, logFields: logFields}
	q.query = q.c.db.Rebind(query)
	q.args = args
	return q
}

// WithSelector construct query from select selector
func (c *querierConstructor) WithSelector(s Selector, logFields ...interface{}) Querier {
	return &sbQuerier{querier: &querier{c: c, logFields: logFields}, selector: s}
}

// a NOP mapper, it will return original data
func (q *querier) nopMapper(src map[string]interface{}, fm FieldMapSelector) map[string]interface{} {
	return src
}

func (q *querier) execMapper(src map[string]interface{}, fm FieldMapSelector) map[string]interface{} {
	dest := make(map[string]interface{})
	for key, val := range src {
		if nkey, nval, ok := fm(key, val); ok {
			dest[nkey] = nval
		} else {
			q.c.log.Warnw("field not found in map column mapper", append(q.logFields,
				"field", nkey,
				"value", nval)...)
		}
	}
	return dest
}

// One fetch one record into struct or primitive type
func (q *querier) One(ctx context.Context, dest interface{}) error {
	q.logIfDebug("fetch one record")
	if q.err == nil {
		q.err = q.c.db.GetContext(ctx, dest, q.query, q.args...)
	}

	q.logIfError("fetch one record error")
	return q.err
}

// OneMap fetch single record int map[string]interface{}
func (q *querier) OneMap(ctx context.Context, fm FieldMapSelector) (map[string]interface{}, error) {
	q.logIfDebug("fetch one record to map")
	var dest map[string]interface{}
	if q.err == nil {
		dest = make(map[string]interface{})
		row := q.c.db.QueryRowxContext(ctx, q.query, q.args...)
		q.err = row.MapScan(dest)
		if q.err == nil && fm != nil {
			dest = q.execMapper(dest, fm)
		}
	}

	q.logIfError("fetch one record to map error")
	return dest, q.err
}

// Many fetch several records into slice.
func (q *querier) Many(ctx context.Context, dest interface{}) error {
	q.logIfDebug("fetch many record")
	if q.err == nil {
		q.err = q.c.db.SelectContext(ctx, dest, q.query, q.args...)
	}

	q.logIfError("fetch many record error")
	return q.err
}

// ManyMap fetch several records into slice of map[string]interface{}
func (q *querier) ManyMap(ctx context.Context, fm FieldMapSelector) (MapSlice, error) {
	q.logIfDebug("fetch many record to map")
	var results MapSlice
	if q.err == nil {
		var rows *sqlx.Rows
		rows, q.err = q.c.db.QueryxContext(ctx, q.query, q.args...)
		if q.err == nil {
			defer rows.Close()
			mapper := q.nopMapper
			if fm != nil {
				mapper = q.execMapper
			}

			// iterate over each row
			for rows.Next() {
				d := make(map[string]interface{})
				if q.err = rows.MapScan(d); q.err != nil {
					break
				}
				results = append(results, mapper(d, fm))
			}
			q.err = rows.Err()
		}
	}

	q.logIfError("fetch many record to map error")
	return results, q.err
}

func (q *querier) Count(ctx context.Context) (int64, error) {
	q.logIfDebug("count record")
	var count int64
	if q.err == nil {
		q.err = q.c.db.GetContext(ctx, &count, q.query, q.args...)
	}
	q.logIfError("count record")
	return count, q.err
}

// Err return first error encountered during processing.
// If error encountered, further processing will stop
func (q *querier) Err() error {
	return q.err
}

func (q *querier) logIfError(msg string) {
	if q.err != nil {
		q.c.log.Errorw(msg, append(q.logFields,
			"query", q.query,
			"args", q.args,
			"error", q.err.Error())...)
	}
}

func (q *querier) logIfDebug(msg string) {
	if q.c.log.HasLevel(logger.DebugLevel) {
		q.c.log.Debugw(msg, append(q.logFields,
			"query", q.query,
			"args", q.args)...)
	}
}

// One fetch one record into struct or primitive type
func (q *sbQuerier) One(ctx context.Context, dest interface{}) error {
	q.query, q.args, q.err = q.selector.Select()
	return q.querier.One(ctx, dest)
}

// OneMap fetch single record int map[string]interface{}
func (q *sbQuerier) OneMap(ctx context.Context, fm FieldMapSelector) (map[string]interface{}, error) {
	q.query, q.args, q.err = q.selector.Select()
	return q.querier.OneMap(ctx, fm)
}

// Many fetch several records into slice.
func (q *sbQuerier) Many(ctx context.Context, dest interface{}) error {
	q.query, q.args, q.err = q.selector.Select()
	return q.querier.Many(ctx, dest)
}

// ManyMap fetch several records into slice of map[string]interface{}
func (q *sbQuerier) ManyMap(ctx context.Context, fm FieldMapSelector) (MapSlice, error) {
	q.query, q.args, q.err = q.selector.Select()
	return q.querier.ManyMap(ctx, fm)
}

func (q *sbQuerier) Count(ctx context.Context) (int64, error) {
	q.query, q.args, q.err = q.selector.Count()
	return q.querier.Count(ctx)
}
