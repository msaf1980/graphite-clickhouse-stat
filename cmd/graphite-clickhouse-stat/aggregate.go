package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/goccy/go-json"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/aggregate"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/utils"
	"github.com/spf13/cobra"
)

func printReport(name string, sortKey string, n int) {
	fmt.Printf("      Top %d report: %s (sort by %s)\n\n", n, name, sortKey)
}

func printHeaderIndexAgg() {
	fmt.Printf("%6s | %6s | %14s | %10s | %6s | %6s | %6s | %6s | %6s | %s\n",
		"count", "errors", "metric", "sum", "median", "p90", "p95", "p99", "max", "query",
	)
}

func printHeaderDataAgg() {
	fmt.Printf("%6s | %6s | %14s | %10s | %6s | %6s | %6s | %6s | %6s | %s\n",
		"count", "errors", "metric", "sum", "median", "p90", "p95", "p99", "max", "query",
	)
}

func printCount(n, errors int64) {
	fmt.Printf("%6d | %6d |", n, errors)
}

func printIndexHeader() {
	fmt.Printf("%15s |", "index cache")
}

func printIndexHitHeader() {
	fmt.Printf("%6s | %6s |", "hits", "miss")
}

func printIndexHit(hits, miss int64) {
	fmt.Printf("%6d | %6d |", hits, miss)
}

func printIndexFooter() {
	fmt.Printf("%15s |", "---------------")
}

func printCountBlank() {
	fmt.Printf("%15s |", "")
}

func printTableAndDuration(requestType, table, duration string) {
	fmt.Printf(" %-14s %20s  %s\n", requestType, table, duration)
}

func printTarget(target string) {
	fmt.Printf(" %s\n", target)
}

func printEndline() {
	fmt.Println()
}

func printAggNode(name string, aggNode *aggregate.AggNode, prec int) {
	fmt.Printf(" %14s | %10s | %6s | %6s | %6s | %6s | %6s |", name,
		utils.FormatFloat64(aggNode.Sum, prec), utils.FormatFloat64(aggNode.Median, prec), utils.FormatFloat64(aggNode.P90, prec),
		utils.FormatFloat64(aggNode.P95, prec), utils.FormatFloat64(aggNode.P99, prec), utils.FormatFloat64(aggNode.Max, prec),
	)
}

func printAggTimeNode(name string, aggNode *aggregate.AggNode) {
	fmt.Printf(" %14s | %10.2f | %6.2f | %6.2f | %6.2f | %6.2f | %6.2f |", name, aggNode.Sum, aggNode.Median, aggNode.P90, aggNode.P95, aggNode.P99, aggNode.Max)
}

type AggregateConfig struct {
	Top    int
	AggKey aggregate.AggSort
}

var aggConfig AggregateConfig

func aggRun(cmd *cobra.Command, args []string) {
	if len(args) > 0 {
		fmt.Fprintf(os.Stderr, "unhandled args: %v\n", args)
		cmd.Help()
		os.Exit(1)
	}

	if aggConfig.Top <= 0 {
		fmt.Fprintf(os.Stderr, "top must be > 0\n")
		os.Exit(1)
	}

	var logEntry map[string]interface{}
	queries := make(map[string]*stat.Stat)

	statIndexSum := aggregate.NewStatIndexSummary()
	statDataSum := aggregate.NewStatDataSummary()

	// printHeader()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		err := json.Unmarshal(scanner.Bytes(), &logEntry)
		if err == nil {
			id := stat.LogEntryProcess(logEntry, queries)
			if len(id) > 0 {
				s := queries[id]
				statIndexSum.Append(s)
				statDataSum.Append(s)

				delete(queries, id)
			}
		}
	}

	statIndexAgg := statIndexSum.Aggregate()

	switch aggConfig.AggKey {
	case aggregate.AggSortP99:
		sort.SliceStable(statIndexAgg, func(i, j int) bool {
			return aggregate.LessIndexAggP99ByRows(&statIndexAgg[i], &statIndexAgg[j])
		})
	case aggregate.AggSortP95:
		sort.SliceStable(statIndexAgg, func(i, j int) bool {
			return aggregate.LessIndexAggP95ByRows(&statIndexAgg[i], &statIndexAgg[j])
		})
	case aggregate.AggSortP90:
		sort.SliceStable(statIndexAgg, func(i, j int) bool {
			return aggregate.LessIndexAggP90ByRows(&statIndexAgg[i], &statIndexAgg[j])
		})
	case aggregate.AggSortMedian:
		sort.SliceStable(statIndexAgg, func(i, j int) bool {
			return aggregate.LessIndexAggMedianByRows(&statIndexAgg[i], &statIndexAgg[j])
		})
	case aggregate.AggSortSum:
		sort.SliceStable(statIndexAgg, func(i, j int) bool {
			return aggregate.LessIndexAggSumByRows(&statIndexAgg[i], &statIndexAgg[j])
		})
	default:
		sort.SliceStable(statIndexAgg, func(i, j int) bool {
			return aggregate.LessIndexAggMaxByRows(&statIndexAgg[i], &statIndexAgg[j])
		})
	}

	n := len(statIndexAgg) - aggConfig.Top
	if n < 0 {
		n = 0
	}

	printReport("Index queries", aggConfig.AggKey.String(), aggConfig.Top)
	printHeaderIndexAgg()
	printFooter()
	for i := n; i < len(statIndexAgg); i++ {
		aggStat := statIndexAgg[i]
		aggSum := statIndexSum[aggStat.Key]

		printCount(aggSum.N, aggSum.Errors)
		printAggTimeNode("time", &aggStat.IndexTime)
		printTableAndDuration(aggStat.Key.RequestType, aggStat.Key.IndexTable, aggStat.Key.Duration.String())

		printCountBlank()
		printAggNode("metrics", &aggStat.Metrics, 0)
		queryIds := aggSum.IndexQueryIds
		fmt.Printf(" IDs: %s", queryIds[0])
		if len(queryIds) > 2 {
			fmt.Printf(" .. %s", queryIds[len(queryIds)/2])
		}
		if len(queryIds) > 1 {
			fmt.Printf(" .. %s", queryIds[len(queryIds)-1])
		}
		printEndline()

		printCountBlank()
		printAggNode("chrows", &aggStat.IndexReadRows, 0)
		printEndline()

		printCountBlank()
		printAggNode("chsize", &aggStat.IndexReadBytes, 0)
		printTarget(aggStat.Key.Target)

		if len(aggSum.RequestErrors) > 0 {
			for respStatus, errMap := range aggSum.RequestErrors {
				fmt.Printf(" Errors with status %d:\n", respStatus)
				for err, count := range errMap {
					fmt.Printf("        %d: %s\n", count, err)
				}
			}
		}

		printFooter()
	}
	printEndline()

	// Index queries with errors

	sort.SliceStable(statIndexAgg, func(i, j int) bool {
		return aggregate.LessIndexAggSumByErrors(&statIndexAgg[i], &statIndexAgg[j])
	})

	printReport("Index queries with errors", aggConfig.AggKey.String(), aggConfig.Top)
	printHeaderIndexAgg()
	printFooter()
	for i := n; i < len(statIndexAgg); i++ {
		aggStat := statIndexAgg[i]
		if aggStat.Errors > 0 {
			aggSum := statIndexSum[aggStat.Key]

			printCount(aggSum.N, aggSum.Errors)
			printAggTimeNode("time", &aggStat.IndexTime)
			printTableAndDuration(aggStat.Key.RequestType, aggStat.Key.IndexTable, aggStat.Key.Duration.String())

			printCountBlank()
			printAggNode("metrics", &aggStat.Metrics, 0)
			queryIds := aggSum.IndexQueryIds
			fmt.Printf(" IDs: %s", queryIds[0])
			if len(queryIds) > 2 {
				fmt.Printf(" .. %s", queryIds[len(queryIds)/2])
			}
			if len(queryIds) > 1 {
				fmt.Printf(" .. %s", queryIds[len(queryIds)-1])
			}
			printEndline()

			printCountBlank()
			printAggNode("chrows", &aggStat.IndexReadRows, 0)
			printEndline()

			printCountBlank()
			printAggNode("chsize", &aggStat.IndexReadBytes, 0)
			printTarget(aggStat.Key.Target)

			if len(aggSum.RequestErrors) > 0 {
				for respStatus, errMap := range aggSum.RequestErrors {
					fmt.Printf(" Errors with status %d:\n", respStatus)
					for err, count := range errMap {
						fmt.Printf("        %d: %s\n", count, err)
					}
				}
			}

			printFooter()
		}
	}
	printEndline()

	// Data queries

	statDataAgg := statDataSum.Aggregate()

	switch aggConfig.AggKey {
	case aggregate.AggSortP99:
		sort.SliceStable(statDataAgg, func(i, j int) bool {
			return aggregate.LessDataAggP99ByRows(&statDataAgg[i], &statDataAgg[j])
		})
	case aggregate.AggSortP95:
		sort.SliceStable(statDataAgg, func(i, j int) bool {
			return aggregate.LessDataAggP95ByRows(&statDataAgg[i], &statDataAgg[j])
		})
	case aggregate.AggSortP90:
		sort.SliceStable(statDataAgg, func(i, j int) bool {
			return aggregate.LessDataAggP90ByRows(&statDataAgg[i], &statDataAgg[j])
		})
	case aggregate.AggSortMedian:
		sort.SliceStable(statDataAgg, func(i, j int) bool {
			return aggregate.LessDataAggMedianByRows(&statDataAgg[i], &statDataAgg[j])
		})
	case aggregate.AggSortSum:
		sort.SliceStable(statDataAgg, func(i, j int) bool {
			return aggregate.LessDataAggSumByRows(&statDataAgg[i], &statDataAgg[j])
		})
	default:
		sort.SliceStable(statDataAgg, func(i, j int) bool {
			return aggregate.LessDataAggMaxByRows(&statDataAgg[i], &statDataAgg[j])
		})
	}

	n = len(statDataAgg) - aggConfig.Top
	if n < 0 {
		n = 0
	}

	printReport("Data queries", aggConfig.AggKey.String(), aggConfig.Top)
	printHeaderDataAgg()
	printFooter()
	for i := n; i < len(statDataAgg); i++ {
		aggStat := statDataAgg[i]
		aggSum := statDataSum[aggStat.Key]

		printCount(aggSum.N, aggSum.Errors)
		printAggTimeNode("time", &aggStat.DataTime)
		printTableAndDuration(aggStat.Key.RequestType, aggStat.Key.DataTable, aggStat.Key.Duration.String())

		printIndexFooter()
		printAggNode("metrics", &aggStat.Metrics, 0)
		queryIds := aggSum.DataQueryIds
		fmt.Printf(" IDs: %s", queryIds[0])
		if len(queryIds) > 2 {
			fmt.Printf(" .. %s", queryIds[len(queryIds)/2])
		}
		if len(queryIds) > 1 {
			fmt.Printf(" .. %s", queryIds[len(queryIds)-1])
		}
		printEndline()

		printIndexHeader()
		printAggNode("points", &aggStat.Points, 0)
		printEndline()

		printIndexHitHeader()
		printAggNode("bytes", &aggStat.Bytes, 0)
		printEndline()

		printIndexFooter()
		printAggNode("chrows", &aggStat.DataReadRows, 0)
		printEndline()

		printIndexHit(aggSum.IndexCacheHit, aggSum.IndexCacheMiss)
		printAggNode("chsize", &aggStat.DataReadBytes, 0)
		printTarget(aggStat.Key.Target)

		if len(aggSum.RequestErrors) > 0 {
			for respStatus, errMap := range aggSum.RequestErrors {
				fmt.Printf(" Errors with status %d:\n", respStatus)
				for err, count := range errMap {
					fmt.Printf("        %d: %s\n", count, err)
				}
			}
		}

		printFooter()
	}
	printEndline()

	// Data queries with errors

	sort.SliceStable(statDataAgg, func(i, j int) bool {
		return aggregate.LessDataAggSumByErrors(&statDataAgg[i], &statDataAgg[j])
	})

	printReport("Data queries with errors", aggConfig.AggKey.String(), aggConfig.Top)
	printHeaderDataAgg()
	printFooter()
	for i := n; i < len(statDataAgg); i++ {
		aggStat := statDataAgg[i]
		aggSum := statDataSum[aggStat.Key]

		printCount(aggSum.N, aggSum.Errors)
		printAggTimeNode("time", &aggStat.DataTime)
		printTableAndDuration(aggStat.Key.RequestType, aggStat.Key.DataTable, aggStat.Key.Duration.String())

		printIndexFooter()
		printAggNode("metrics", &aggStat.Metrics, 0)
		queryIds := aggSum.DataQueryIds
		fmt.Printf(" IDs: %s", queryIds[0])
		if len(queryIds) > 2 {
			fmt.Printf(" .. %s", queryIds[len(queryIds)/2])
		}
		if len(queryIds) > 1 {
			fmt.Printf(" .. %s", queryIds[len(queryIds)-1])
		}
		printEndline()

		printIndexHeader()
		printAggNode("points", &aggStat.Points, 0)
		printEndline()

		printIndexHitHeader()
		printAggNode("bytes", &aggStat.Bytes, 0)
		printEndline()

		printIndexFooter()
		printAggNode("chrows", &aggStat.DataReadRows, 0)
		printEndline()

		printIndexHit(aggSum.IndexCacheHit, aggSum.IndexCacheMiss)
		printAggNode("chsize", &aggStat.DataReadBytes, 0)
		printTarget(aggStat.Key.Target)

		if len(aggSum.RequestErrors) > 0 {
			for respStatus, errMap := range aggSum.RequestErrors {
				fmt.Printf(" Errors with status %d:\n", respStatus)
				for err, count := range errMap {
					fmt.Printf("        %d: %s\n", count, err)
				}
			}
		}

		printFooter()
	}
	printEndline()
}

func aggFlags(rootCmd *cobra.Command) {
	cmd := &cobra.Command{
		Use:   "aggregate",
		Short: "Read from stdin and print top queries summary stat ",
		Run:   aggRun,
	}

	cmd.Flags().SortFlags = false

	cmd.Flags().IntVarP(&aggConfig.Top, "top", "n", 10, "top queries")
	cmd.Flags().VarP(&aggConfig.AggKey, "agg", "a", "agg top key ("+strings.Join(aggregate.AggSortStrings(), " | ")+") ")

	rootCmd.AddCommand(cmd)
}
