package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/msaf1980/go-clipper"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/aggregate"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/utils"
)

type AggConfig struct {
	Top int

	Sort aggregate.RequestSort
	Key  aggregate.AggSortKey

	IndexSort aggregate.IndexSort
	// IndexKey  aggregate.AggSortKey

	InFile  string
	OutFile string

	From  time.Time
	Until time.Time
}

var aggConfig AggConfig

func printReport(name string, sort, sortKey string, n int) {
	fmt.Printf("      Top %d report: %s (sort by %s %s)\n\n", n, name, sort, sortKey)
}

func printLabelHeader() {
	fmt.Printf("%16s | %15s | %s\n", "duration label", "offset label", "request_type")
	printFooter()
}

func printQueryHeader() {
	fmt.Printf("%16s | %15s | %s\n", "duration", "offset", "query")
	printFooter()
}

func printIndexStatHeader() {
	fmt.Printf("%16s | %6s | %6s | %33s | %32s |\n",
		"N", "err%", "chit%", "sample req id", "err req id",
	)
}

func printQueryStatHeader() {
	fmt.Printf("%16s | %6s | %6s | %6s | %6s | %33s | %32s |\n",
		"N", "err%", "ierr%", "derr%", "chit%", "sample req id", "err req id",
	)
}

func printLabel(label aggregate.LabelKey) {
	fmt.Printf("%16s | %15s | %-165s |\n", label.DurationLabel, label.OffsetLabel, label.RequestType)
}

func printQueries(queries []aggregate.StatQuery) {
	if len(queries) == 0 {
		fmt.Printf("%16s | %15s | %s\n", "", "", "Incomplete log: no queries")
	} else {
		for _, q := range queries {
			fmt.Printf("%16s | %15s | %s\n", q.DurationLabel, q.Offset, q.Query)
		}
	}
}

func printAggNodeHeader() {
	fmt.Printf("%16s | %15s | %15s | %15s | %15s | %15s\n",
		"metric", "p50", "p90", "p95", "p99", "max",
	)
}

func printSmallFooter() {
	fmt.Print("--------------------------------------------------------------------------------------------------------------------------\n")
}

func printEndline() {
	fmt.Println()
}

// func printAggNodePcnt(name string, node aggregate.AggNode) {
// 	fmt.Printf("%-14s | %6f | %6f | %6f | %6f | %6f\n", name, node.P50, node.P90, node.P95, node.P99, node.Max)
// }

func printAggNode(name string, aggNode *aggregate.AggNode, prec int) {
	if aggNode.Init {
		fmt.Printf("%16s | %15s | %15s | %15s | %15s | %15s\n", name,
			utils.FormatFloat64Z(aggNode.P50, prec), utils.FormatFloat64Z(aggNode.P90, prec),
			utils.FormatFloat64Z(aggNode.P95, prec), utils.FormatFloat64Z(aggNode.P99, prec),
			utils.FormatFloat64Z(aggNode.Max, prec),
		)
	}
}

func printIndexes(idxs []*aggregate.StatIndexAggNode, n int, indexSort aggregate.IndexSort, key aggregate.AggSortKey) {
	aggregate.SortIndexAgg(idxs, indexSort, key)
	if n < len(idxs) {
		idxs = idxs[len(idxs)-n:]
	}

	for _, s := range idxs {
		printQueries(s.Queries)
		printSmallFooter()
		fmt.Printf("%16s | %6s | %6s | %33s | %s\n",
			utils.FormatInt64(s.N), utils.FormatPcnt(s.ErrorsPcnt), utils.FormatPcnt(s.IndexCacheHitPcnt), s.SampleId, s.ErrorId,
		)
		printSmallFooter()
		printAggNode("times", &s.Times, 2)
		printAggNode("metrics", &s.Metrics, 2)
		printAggNode("read_rows", &s.ReadRows, 2)
		printAggNode("read_bytes", &s.ReadBytes, 2)
		printFooter()
	}
}

func printRequests(qs []*aggregate.StatRequestAggNode, n int, sort aggregate.RequestSort, key aggregate.AggSortKey) {
	// aggregate.SortIndexAgg(idxs, indexSort, key)
	if n < len(qs) {
		qs = qs[len(qs)-n:]
	}

	for _, s := range qs {
		printQueries(s.Queries)
		printSmallFooter()
		fmt.Printf("%16s | %6s | %6s | %6s | %6s | %33s | %s\n",
			utils.FormatInt64(s.N), utils.FormatPcnt(s.ErrorsPcnt),
			utils.FormatPcnt(s.IndexErrorsPcnt), utils.FormatPcnt(s.DataErrorsPcnt),
			utils.FormatPcnt(s.IndexCacheHitPcnt), s.SampleId, s.ErrorId,
		)
		printSmallFooter()
		printAggNode("qtimes", &s.QueryTimes, 2)
		printAggNode("rtimes", &s.RequestTimes, 2)
		printAggNode("metrics", &s.Metrics, 2)
		printAggNode("points", &s.Points, 2)
		printAggNode("read_rows", &s.ReadRows, 2)
		printAggNode("read_bytes", &s.ReadBytes, 2)
		printAggNode("index_read_rows", &s.IndexReadRows, 2)
		printAggNode("index_read_bytes", &s.IndexReadBytes, 2)
		printAggNode("data_read_rows", &s.DataReadRows, 2)
		printAggNode("data_read_bytes", &s.DataReadBytes, 2)
		printFooter()
	}
}

func loadAggStat(n int, sort aggregate.RequestSort, key aggregate.AggSortKey, inPath string, from, until int64) (*aggregate.StatAggSum, error) {
	var (
		in         io.ReadCloser
		err        error
		aggStatSum *aggregate.StatAggSum
	)
	if inPath == "" {
		in = os.Stdin
	} else if strings.HasSuffix(inPath, ".json") {
		var b []byte
		if b, err = os.ReadFile(inPath); err == nil {
			var aggSum aggregate.StatAggSumSlice
			if err = json.Unmarshal(b, &aggSum); err == nil {
				aggStatSum = aggregate.NewAggSummary()
				for _, idx := range aggSum.Index {
					label := aggregate.BuildLabelKey(idx.IndexKey)
					aggs, ok := aggStatSum.Index[label]
					if !ok {
						aggs = make([]*aggregate.StatIndexAggNode, 0, 24)
					}
					aggs = append(aggs, idx)
					aggStatSum.Index[label] = aggs
				}
				for _, req := range aggSum.Requests {
					label := aggregate.BuildLabelKey(req.DataKey)
					aggs, ok := aggStatSum.Requests[label]
					if !ok {
						aggs = make([]*aggregate.StatRequestAggNode, 0, 24)
					}
					aggs = append(aggs, req)
					aggStatSum.Requests[label] = aggs
				}
			}
		}
		return aggStatSum, err
	} else {
		if in, err = os.Open(inPath); err != nil {
			return nil, err
		}
		defer in.Close()
	}

	queries := make(map[string]*stat.Stat)
	var logEntry map[string]interface{}

	statSum := aggregate.NewStatSummary()

	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		stat.ResetLogEntry(logEntry)
		line := scanner.Bytes()
		err := json.Unmarshal(line, &logEntry)
		if err == nil {
			id := stat.LogEntryProcess(logEntry, queries)
			if id != "" {
				stat := queries[id]

				add := true
				if from > 0 && stat.TimeStamp < from {
					add = false
				}
				if add && until > 0 && stat.TimeStamp >= until {
					add = false
				}
				if add {
					statSum.Append(stat)
				}

				delete(queries, id)
			}
		}
	}

	aggStatSum = statSum.Aggregate()

	return aggStatSum, nil
}

func aggRun() error {
	if aggConfig.Top <= 0 {
		return errors.New("top must be > 0")
	}
	switch aggConfig.Sort {
	case aggregate.RequestSortQTime, aggregate.RequestSortRTime, aggregate.RequestSortDTime:
		aggConfig.IndexSort = aggregate.IndexSortTime
	case aggregate.RequestSortReadRows, aggregate.RequestSortIndexReadRows, aggregate.RequestSortDataReadRows:
		aggConfig.IndexSort = aggregate.IndexSortReadRows
	case aggregate.RequestSortQueries:
		aggConfig.IndexSort = aggregate.IndexSortQueries
	case aggregate.RequestSortErrors:
		aggConfig.IndexSort = aggregate.IndexSortErrors
	default:
		return fmt.Errorf("invalid sort %d", aggConfig.Sort)
	}
	if aggConfig.OutFile != "" && !strings.HasSuffix(aggConfig.OutFile, ".json") {
		return errors.New("only json supported for out")
	}

	var (
		from  int64
		until int64
	)
	if !aggConfig.From.IsZero() {
		from = aggConfig.From.UnixNano()
	}
	if !aggConfig.Until.IsZero() {
		until = aggConfig.Until.UnixNano()
	}

	aggStatSum, err := loadAggStat(aggConfig.Top, aggConfig.Sort, aggConfig.Key, aggConfig.InFile, from, until)
	if err != nil {
		return err
	}

	if aggConfig.OutFile == "" {
		// Index queries
		printReport("Index queries", aggConfig.IndexSort.String(), aggConfig.Key.String(), aggConfig.Top)

		printLabelHeader()
		labels := aggStatSum.IndexLabels()
		for _, label := range labels {
			idxs := aggStatSum.Index[label]
			printLabelFooter()
			printLabel(label)
			printLabelFooter()
			printQueryHeader()
			printIndexStatHeader()
			printFooter()
			printAggNodeHeader()
			printFooter()

			printIndexes(idxs, aggConfig.Top, aggConfig.IndexSort, aggConfig.Key)
		}
		printEndline()

		// 	Queries stat

		printReport("Queries", aggConfig.Sort.String(), aggConfig.Key.String(), aggConfig.Top)

		printLabelHeader()
		labels = aggStatSum.RequestLabels()
		for _, label := range labels {
			qs := aggStatSum.Requests[label]
			printLabelFooter()
			printLabel(label)
			printLabelFooter()
			printQueryHeader()
			printQueryStatHeader()
			printFooter()
			printAggNodeHeader()
			printFooter()

			printRequests(qs, aggConfig.Top, aggConfig.Sort, aggConfig.Key)
		}
		printEndline()
		return nil
	} else {
		var b []byte
		aggStats := aggStatSum.Slice()
		if b, err = json.Marshal(&aggStats); err == nil {
			err = os.WriteFile(aggConfig.OutFile, b, 0644)
		}
		return err
	}
}

func registerAggregateCmd(registry *clipper.Registry) {
	aggCommand, _ := registry.RegisterWithCallback("aggregate", "read and print top queries aggregated stat", aggRun)

	aggCommand.AddInt("top", "n", 10, &aggConfig.Top, "print top queries")

	aggCommand.AddValue("sort", "s", &aggConfig.Sort, false, "aggregate top sort by ("+strings.Join(aggregate.RequestSortStrings(), " | ")+") ")
	aggCommand.AddValue("key", "k", &aggConfig.Key, false, "aggregate top key ("+strings.Join(aggregate.SortKeyStrings(), " | ")+") ")

	// aggCommand.AddValue("index-sort", "S", &aggConfig.IndexSort, false, "aggregate top sort by ("+strings.Join(aggregate.IndexSortStrings(), " | ")+") ")
	// aggCommand.AddValue("index-key", "K", &aggConfig.IndexKey, false, "aggregate top key ("+strings.Join(aggregate.SortKeyStrings(), " | ")+") ")

	aggCommand.AddString("input", "i", "", &aggConfig.InFile, "input log/json file or stdin")

	aggCommand.AddString("output", "o", "", &aggConfig.OutFile, "output json file")

	aggCommand.AddTime("from", "f", time.Time{}, &aggConfig.From, dateTimeLayout, "start time (UTC)")
	aggCommand.AddTime("until", "u", time.Time{}, &aggConfig.Until, dateTimeLayout, "end time (UTC)")
}
