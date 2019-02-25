package sqlcursor

import (
	"fmt"
	"strings"
)

const (
	xlt      = "<"
	xgt      = ">"
	xor      = "OR"
	xmatch   = "$match"
	xsort    = "ORDER BY"
	xlimit   = "LIMIT"
	xwhere   = "WHERE"
	emptyStr = ""
	ASC      = " ASC"
	DESC     = " DESC"
)

type sortField struct {
	name string
	dir  string
	comp string
}

type QueryCursorBuilder struct {
	sortFields  []sortField
	sorts       []int
	sortNames   []string
	values      []interface{}
	bindValues  []interface{}
	limit       int64
	query       string
	where       string
	group       string
	token       string
	isPrevToken bool
	isWhere     bool
}

func NewBuilder(limit int, token string) *QueryCursorBuilder {
	c := &QueryCursorBuilder{
		sortFields: make([]sortField, 0, 3),
	}

	if limit < 0 {
		c.limit = 0
	} else {
		c.limit = int64(limit)
	}

	c.parseToken(token)
	c.token = token

	return c
}

func (c *QueryCursorBuilder) Sort(field string, sort string) *QueryCursorBuilder {
	sortField := sortField{
		name: field,
		dir:  sort,
	}

	if c.isPrevToken {
		if sort == DESC {
			sortField.comp = xgt
		} else {
			sortField.comp = xlt
		}
	} else {
		if sort == DESC {
			sortField.comp = xlt
		} else {
			sortField.comp = xgt
		}
	}

	c.sortFields = append(c.sortFields, sortField)

	return c
}

func (c *QueryCursorBuilder) Query(query string) *QueryCursorBuilder {
	c.query = query

	return c
}

func (c *QueryCursorBuilder) Where(where string) *QueryCursorBuilder {
	c.where = "WHERE (" + where + ")"
	c.isWhere = true

	return c
}

func (c *QueryCursorBuilder) Group(group string) *QueryCursorBuilder {
	c.group = "GROUP BY " + group

	return c
}

func (c *QueryCursorBuilder) Build(binds ...interface{}) (string, []interface{}, error) {
	if err := c.validate(); err != nil {
		return emptyStr, nil, err
	}

	n := len(c.sortFields)

	limit := emptyStr

	if c.limit > 0 {
		limit = fmt.Sprintf("LIMIT %d", c.limit+1)
	}

	sortFields := make([]string, n)
	for i := 0; i < n; i++ {
		sortFields[i] = c.sortFields[i].name + c.sortFields[i].dir
	}
	sort := fmt.Sprintf("ORDER BY %s", strings.Join(sortFields, ", "))

	if !c.isWhere && c.token != emptyStr {
		c.where = xwhere
	}

	if len(binds) > 0 {
		if c.values == nil {
			c.bindValues = binds
		} else {
			c.bindValues = make([]interface{}, 0, len(binds)+len(c.values))
			c.bindValues = append(c.bindValues, binds...)
		}
	} else {
		c.bindValues = make([]interface{}, 0, (n*2)-1)
	}

	if n == 1 && c.values != nil {
		return fmt.Sprintf("%s %s %s %s %s %s", c.query, c.where, c.createSingleQuery(), c.group, sort, limit), c.bindValues, nil
	} else if c.values != nil {
		return fmt.Sprintf("%s %s %s %s %s %s", c.query, c.where, c.createOrQuery(n), c.group, sort, limit), c.bindValues, nil
	}

	return fmt.Sprintf("%s %s %s %s %s", c.query, c.where, c.group, sort, limit), c.bindValues, nil
}

func (c *QueryCursorBuilder) validate() error {
	if c.query == "" {
		return ErrNoQuery
	}

	if len(c.sortFields) == 0 {
		return ErrNoSort
	}

	if c.token != emptyStr {
		if c.values == nil {
			return ErrNoDataInToken
		}

		if len(c.values) < len(c.sortFields) {
			return ErrInsufficientTokenValue
		}
	}

	return nil
}

func (c *QueryCursorBuilder) createSingleQuery() string {
	c.bindValues = append(c.bindValues, c.values[0])
	if c.isWhere {
		return fmt.Sprintf("AND (%s %s ?)", c.sortFields[0].name, c.sortFields[0].comp)
	}

	return fmt.Sprintf("(%s %s ?)", c.sortFields[0].name, c.sortFields[0].comp)
}

func (c *QueryCursorBuilder) createOrQuery(n int) string {
	orQuery := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if i == 0 {
			c.bindValues = append(c.bindValues, c.values[0])
			orQuery = append(orQuery, fmt.Sprintf("%s %s ?", c.sortFields[0].name, c.sortFields[0].comp))
		} else {
			c.bindValues = append(c.bindValues, c.values[0], c.values[i])
			orQuery = append(orQuery, fmt.Sprintf("(%s %s ? AND %s %s ?)", c.sortFields[0].name, c.sortFields[0].comp, c.sortFields[i].name, c.sortFields[i].comp))
		}
	}

	if c.isWhere {
		return fmt.Sprintf("AND (%s)", strings.Join(orQuery, " OR "))
	}

	return fmt.Sprintf("(%s)", strings.Join(orQuery, " OR "))
}

func (c *QueryCursorBuilder) parseToken(token string) {
	if token == emptyStr {
		return
	}

	cf := decodeToken(token)
	if cf == nil {
		return
	}

	if cf[0].(int64) == 1 {
		c.isPrevToken = true
	}
	c.values = make([]interface{}, 0, len(cf))
	for i := 1; i < len(cf); i++ {
		c.values = append(c.values, cf[i])
	}
}
