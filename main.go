package main

import (
	"math/rand"
	"time"

	"github.com/cube2222/octosql/plugins"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	plugins.Run(Creator)
}
