package mongocursor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryWithWhereAndOneToken(t *testing.T) {
	nextToken, _, _ := CreateToken("", 3, 4,
		func(index int) []interface{} {
			return []interface{}{"a4"}
		},
		func(index int) {},
	)

	query, values, err := NewBuilder(3, nextToken).
		Sort("id", ASC).
		Query("SELECT * FROM user").
		Where("id = `?`").
		Build()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM user WHERE (id = `?`) AND (id > ?) ORDER BY id ASC LIMIT 4", query)
	require.Equal(t, []interface{}{"a4"}, values)
}

func TestQueryWithWhereAndGroupAndOneToken(t *testing.T) {
	nextToken, _, _ := CreateToken("", 3, 4,
		func(index int) []interface{} {
			return []interface{}{"a4"}
		},
		func(index int) {},
	)

	query, values, err := NewBuilder(3, nextToken).
		Sort("id", ASC).
		Query("SELECT * FROM user").
		Where("id = `?`").
		Group("id").
		Build()
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM user WHERE (id = `?`) AND (id > ?) GROUP BY id ORDER BY id ASC LIMIT 4", query)
	require.Equal(t, []interface{}{"a4"}, values)
}

func TestQueryWithWhereAndOneTokenAndBind(t *testing.T) {
	nextToken, _, _ := CreateToken("", 3, 4,
		func(index int) []interface{} {
			return []interface{}{"a4"}
		},
		func(index int) {},
	)

	query, values, err := NewBuilder(3, nextToken).
		Sort("id", ASC).
		Query("SELECT * FROM user").
		Where("id = `?`").
		Build(1)
	require.NoError(t, err)
	require.Equal(t, "SELECT * FROM user WHERE (id = `?`) AND (id > ?) ORDER BY id ASC LIMIT 4", query)
	require.Equal(t, []interface{}{1, "a4"}, values)
}

func TestQueryWithWhereAndToken(t *testing.T) {
	query, values, err := NewBuilder(3, "kwCiYTShMQ==").
		Sort("a", DESC).
		Sort("id", ASC).
		Query("SELECT * FROM user").
		Where("id = `?`").
		Build()
	require.Equal(t, "SELECT * FROM user WHERE (id = `?`) AND (a < ? OR (a < ? AND id > ?)) ORDER BY a DESC, id ASC LIMIT 4", query)
	require.Equal(t, []interface{}{"a4", "a4", "1"}, values)
	require.NoError(t, err)
}

func TestQueryWithWhereAndTokenAndBind(t *testing.T) {
	query, values, err := NewBuilder(3, "kwCiYTShMQ==").
		Sort("a", DESC).
		Sort("id", ASC).
		Query("SELECT * FROM user").
		Where("id = `?`").
		Build(1)
	require.Equal(t, "SELECT * FROM user WHERE (id = `?`) AND (a < ? OR (a < ? AND id > ?)) ORDER BY a DESC, id ASC LIMIT 4", query)
	require.Equal(t, []interface{}{1, "a4", "a4", "1"}, values)
	require.NoError(t, err)
}

func TestQueryWithWhereAndGroupAndTokenAndBind(t *testing.T) {
	query, values, err := NewBuilder(3, "kwCiYTShMQ==").
		Sort("a", DESC).
		Sort("id", ASC).
		Query("SELECT * FROM user").
		Where("id = `?`").
		Group("id").
		Build(1)
	require.Equal(t, "SELECT * FROM user WHERE (id = `?`) AND (a < ? OR (a < ? AND id > ?)) GROUP BY id ORDER BY a DESC, id ASC LIMIT 4", query)
	require.Equal(t, []interface{}{1, "a4", "a4", "1"}, values)
	require.NoError(t, err)
}

func TestQueryWithToken(t *testing.T) {
	query, values, err := NewBuilder(3, "kwCiYTShMQ==").
		Sort("a", DESC).
		Sort("id", ASC).
		Query("SELECT * FROM user").
		Build()
	require.Equal(t, "SELECT * FROM user WHERE (a < ? OR (a < ? AND id > ?)) ORDER BY a DESC, id ASC LIMIT 4", query)
	require.Equal(t, []interface{}{"a4", "a4", "1"}, values)
	require.NoError(t, err)
}

func TestNoToken(t *testing.T) {
	query, values, err := NewBuilder(3, "").
		Sort("a", DESC).
		Sort("id", ASC).
		Query("SELECT * FROM user").
		Build()
	require.Equal(t, "SELECT * FROM user  ORDER BY a DESC, id ASC LIMIT 4", query)
	require.Equal(t, []interface{}{}, values)
	require.NoError(t, err)
}

func TestNoTokenWithWhere(t *testing.T) {
	query, values, err := NewBuilder(3, "").
		Sort("a", DESC).
		Sort("id", ASC).
		Query("SELECT * FROM user").
		Where("id = ?").
		Build()
	require.Equal(t, "SELECT * FROM user WHERE (id = ?) ORDER BY a DESC, id ASC LIMIT 4", query)
	require.Equal(t, []interface{}{}, values)
	require.NoError(t, err)
}

func TestNoTokenWithWhereAndBind(t *testing.T) {
	query, values, err := NewBuilder(3, "").
		Sort("a", DESC).
		Sort("id", ASC).
		Query("SELECT * FROM user").
		Where("id = ?").
		Build(1)
	require.Equal(t, "SELECT * FROM user WHERE (id = ?) ORDER BY a DESC, id ASC LIMIT 4", query)
	require.Equal(t, []interface{}{1}, values)
	require.NoError(t, err)
}
