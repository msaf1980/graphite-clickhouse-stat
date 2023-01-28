package main

import (
	"fmt"
	"os"

	"github.com/msaf1980/go-clipper"
)

func main() {
	registry := clipper.NewRegistry("graphite-clickhouse queries stat tool")

	registry.RegisterHelp("help", "display help", true, true)
	registry.Register("", "display help")

	registerPrintCmd(registry)
	registerTopCmd(registry)
	registerAggregateCmd(registry)

	if _, err := registry.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}
}
