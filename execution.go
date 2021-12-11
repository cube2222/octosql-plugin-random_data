package main

import (
	"fmt"
	"time"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type datasourceExecuting struct {
	data   []map[string]interface{}
	fields []physical.SchemaField
}

func (d *datasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	for i := range d.data {
		values := make([]octosql.Value, len(d.fields))
		for j, field := range d.fields {
			values[j] = getOctoSQLValue(field.Type, d.data[i][field.Name])
		}

		if err := produce(
			ProduceFromExecutionContext(ctx),
			NewRecord(values, false, time.Time{}),
		); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}

	return nil
}

func getOctoSQLValue(t octosql.Type, value interface{}) octosql.Value {
	switch value := value.(type) {
	case int:
		return octosql.NewInt(value)
	case bool:
		return octosql.NewBoolean(value)
	case float64:
		return octosql.NewFloat(value)
	case string:
		return octosql.NewString(value)
	case time.Time:
		return octosql.NewTime(value)
	case map[string]interface{}:
		values := make([]octosql.Value, len(t.Struct.Fields))
		for i, field := range t.Struct.Fields {
			values[i] = getOctoSQLValue(field.Type, value[field.Name])
		}
		return octosql.NewStruct(values)
	case []interface{}:
		if t.List.Element == nil && len(value) > 0 {
			panic("list was expected to be empty (typeless) but isn't")
		}

		elements := make([]octosql.Value, len(value))
		for i := range elements {
			elements[i] = getOctoSQLValue(*t.List.Element, value[i])
		}
		return octosql.NewList(elements)
	case nil:
		return octosql.NewNull()
	}

	panic(fmt.Sprintf("unexhaustive json input value match: %T %+v", value, value))
}
