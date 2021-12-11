package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/cube2222/octosql/octosql"
	_ "github.com/jackc/pgx/stdlib"

	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins/plugin"
)

var tableInternalNames = map[string]string{
	"addresses": "address/random_address",
	"companies": "company/random_company",
	"users":     "users/random_user",
}

func Creator(ctx context.Context, configUntyped plugin.ConfigDecoder) (physical.Database, error) {
	return &Database{}, nil
}

type Database struct {
}

func (d *Database) ListTables(ctx context.Context) ([]string, error) {
	return []string{
		"addresses",
		"companies",
		"users",
	}, nil
}

func (d *Database) GetTable(ctx context.Context, name string) (physical.DatasourceImplementation, physical.Schema, error) {
	internalName, ok := tableInternalNames[name]
	if !ok {
		return nil, physical.Schema{}, fmt.Errorf("table %s not found", name)
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("https://random-data-api.com/api/%s", internalName), nil)
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't create request to get sample data: %w", err)
	}
	req = req.WithContext(ctx)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't get sample data: %w", err)
	}
	defer res.Body.Close()

	var data map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't decode sample data: %w", err)
	}

	fieldNames := make([]string, 0, len(data))
	for k := range data {
		fieldNames = append(fieldNames, k)
	}
	sort.Strings(fieldNames)

	fields := make([]physical.SchemaField, len(fieldNames))
	for i := range fieldNames {
		fields[i] = physical.SchemaField{
			Name: fieldNames[i],
			Type: getOctoSQLType(data[fieldNames[i]]),
		}
	}

	return &datasourcePhysical{
			internalName: internalName,
		},
		physical.Schema{
			Fields:    fields,
			TimeField: -1,
		},
		nil
}

func getOctoSQLType(value interface{}) octosql.Type {
	switch value := value.(type) {
	case int:
		return octosql.Int
	case bool:
		return octosql.Boolean
	case float64:
		return octosql.Float
	case string:
		return octosql.String
	case time.Time:
		return octosql.Time
	case map[string]interface{}:
		fieldNames := make([]string, 0, len(value))
		for k := range value {
			fieldNames = append(fieldNames, k)
		}
		sort.Strings(fieldNames)
		fields := make([]octosql.StructField, len(value))
		for i := range fieldNames {
			fields[i] = octosql.StructField{
				Name: fieldNames[i],
				Type: getOctoSQLType(value[fieldNames[i]]),
			}
		}
		return octosql.Type{
			TypeID: octosql.TypeIDStruct,
			Struct: struct{ Fields []octosql.StructField }{Fields: fields},
		}
	case []interface{}:
		var elementType *octosql.Type
		for i := range value {
			if elementType != nil {
				t := octosql.TypeSum(*elementType, getOctoSQLType(value[i]))
				elementType = &t
			} else {
				t := getOctoSQLType(value[i])
				elementType = &t
			}
		}
		return octosql.Type{
			TypeID: octosql.TypeIDList,
			List: struct {
				Element *octosql.Type
			}{
				Element: elementType,
			},
		}
	case nil:
		return octosql.Null
	}

	panic(fmt.Sprintf("unexhaustive json input value match: %T %+v", value, value))
}
