package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/top"
	"github.com/spf13/cobra"
)

type TopConfig struct {
	Top      int
	Duration time.Duration
	Key      stat.Sort
}

var topConfig TopConfig

func printTop(queries map[string]*stat.Stat, n int, sortKey stat.Sort, nonCompleted bool) {
	stats := top.GetTop(queries, n, sortKey, nonCompleted)
	for _, s := range stats {
		printStat(s.Id, s)
	}
}

func topRun(cmd *cobra.Command, args []string) {
	if len(args) > 0 {
		fmt.Fprintf(os.Stderr, "unhandled args: %v\n", args)
		cmd.Help()
		os.Exit(1)
	}

	if topConfig.Top <= 0 {
		fmt.Fprintf(os.Stderr, "top must be > 0\n")
		os.Exit(1)
	}
	if topConfig.Duration < time.Second {
		fmt.Fprintf(os.Stderr, "flush duration must be >= 1s\n")
		os.Exit(1)
	}

	var logEntry map[string]interface{}
	queries := make(map[string]*stat.Stat)

	printHeader()

	var timeStamp time.Time

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		err := json.Unmarshal(scanner.Bytes(), &logEntry)
		if err == nil {
			id := stat.LogEntryProcess(logEntry, queries)
			if len(id) > 0 {
				s := queries[id]
				t := time.Unix(s.TimeStamp/1000000000, s.TimeStamp%100000000).Truncate(topConfig.Duration)
				if timeStamp.IsZero() {
					timeStamp = t
				} else if timeStamp != t {
					printTop(queries, topConfig.Top, topConfig.Key, false)
					printFooter()
					timeStamp = t
				}
			}
		}
	}

	printTop(queries, topConfig.Top, topConfig.Key, true)
}

func topFlags(rootCmd *cobra.Command) {
	cmd := &cobra.Command{
		Use:   "top",
		Short: "Read from stdin and print top queries stat",
		Run:   topRun,
	}

	cmd.Flags().SortFlags = false

	cmd.Flags().IntVarP(&topConfig.Top, "top", "n", 10, "top queries")
	cmd.Flags().DurationVarP(&topConfig.Duration, "duration", "d", 10*time.Second, "flush duration")
	cmd.Flags().VarP(&topConfig.Key, "key", "k", "top key ("+strings.Join(stat.SortStrings(), " | ")+") ")

	rootCmd.AddCommand(cmd)
}
