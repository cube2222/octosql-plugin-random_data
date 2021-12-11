package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/physical"
)

type datasourceImpl struct {
	internalName string
}

func (i *datasourceImpl) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://random-data-api.com/api/%s?size=%d", i.internalName, rand.Intn(80)+10), nil)
	if err != nil {
		return nil, fmt.Errorf("couldn't create request to get data: %w", err)
	}
	req = req.WithContext(ctx)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("couldn't get data: %w", err)
	}
	defer res.Body.Close()

	var data []map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("couldn't decode data: %w", err)
	}

	return &datasourceExecuting{
		data:   data,
		fields: schema.Fields,
	}, nil
}

func (i *datasourceImpl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}
