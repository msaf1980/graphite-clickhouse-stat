package main

import (
	"log"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   os.Args[0],
		Short: "graphite-clickhouse-stat is a graphite-clickhouse queries stat tool",
	}

	printFlags(rootCmd)
	topFlags(rootCmd)
	aggFlags(rootCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("%s\n", err.Error())
		os.Exit(1)
	}
}
