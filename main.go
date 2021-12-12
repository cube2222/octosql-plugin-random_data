package main

import (
	"math/rand"
	"time"

	"github.com/cube2222/octosql/plugins/plugin"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	plugin.Run(Creator)
}
