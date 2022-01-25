package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/goccy/go-json"
	"github.com/spf13/cobra"
	"github.com/vjeantet/jodaTime"

	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/utils"
)

type PrintConfig struct {
	MinRows      int64
	MinTime      float64
	IndexMinRows int64
	IndexMinTime float64
	Status       utils.Int64Slice
	StatusSkip   utils.Int64Slice
}

var printConfig PrintConfig

func printHeader() {
	fmt.Printf("%12s | %8s | %6s | %7s | %6s | %6s | %6s | %20s | %6s | %6s | %6s |S| %20s | %6s | %6s | %6s |S| %32s | %16s | %16s | %s\n",
		"type", "time",
		"rtime", "metrics", "points", "size", "status",
		"index",
		"time",
		"chrows",
		"chsize",
		"data",
		"time",
		"chrows",
		"chsize",
		"request_id", "index_query", "data_query", "target",
	)
}

func printFooter() {
	fmt.Println("-----------------------------------------------------------------------------------------------------------------------------------------------")
}

func printStat(id string, s *stat.Stat) {
	if s.RequestTime > 0.0 {
		fmt.Printf("%12s | %8s | %6.2f | %7s | %6s | %6s | %6d | %20s | %6.2f | %6s | %6s |%s| %20s | %6.2f | %6s | %6s |%s| %32s | %16s | %16s | %s\n",
			s.RequestType,
			jodaTime.Format("HH:mm:ss", time.Unix(s.TimeStamp/1000000000, 0)),
			s.RequestTime, utils.FormatNumber(int64(s.Metrics)), utils.FormatNumber(int64(s.Points)), utils.FormatNumber(int64(s.Bytes)), s.RequestStatus,
			s.IndexTable, s.IndexTime, utils.FormatNumber(s.IndexReadRows), utils.FormatNumber(s.IndexReadBytes), s.IndexStatus.String(),
			s.DataTable, s.DataTime, utils.FormatNumber(s.DataReadRows), utils.FormatNumber(s.DataReadBytes), s.DataStatus.String(),
			id, s.IndexQueryId, s.DataQueryId, s.Target,
		)
	} else if s.IndexTime+s.DataTime > 0.0 {
		fmt.Printf("%12s | %8s | %6s | %7s | %6s | %6s | %6s | %20s | %6.2f | %6s | %6s |%s| %20s | %6.2f | %6s | %6s |%s| %32s | %16s | %16s | %s\n",
			s.RequestType,
			jodaTime.Format("HH:mm:ss", time.Unix(s.TimeStamp/1000000000, 0)),
			"-", utils.FormatNumber(int64(s.Metrics)), utils.FormatNumber(int64(s.Points)), utils.FormatNumber(int64(s.Bytes)), "-",
			s.IndexTable, s.IndexTime, utils.FormatNumber(s.IndexReadRows), utils.FormatNumber(s.IndexReadBytes), s.IndexStatus.String(),
			s.DataTable, s.DataTime, utils.FormatNumber(s.DataReadRows), utils.FormatNumber(s.DataReadBytes), s.DataStatus.String(),
			id, s.IndexQueryId, s.DataQueryId, s.Target,
		)
	}
}

func printRun(cmd *cobra.Command, args []string) {
	if len(args) > 0 {
		fmt.Fprintf(os.Stderr, "unhandled args: %v\n", args)
		cmd.Help()
		os.Exit(1)
	}
	if len(printConfig.Status) > 0 && len(printConfig.StatusSkip) > 0 {
		fmt.Fprintf(os.Stderr, "status and status-skip can't be coexist")
		cmd.Help()
		os.Exit(1)
	}

	var compare bool
	validStatus := make(map[int64]bool)
	skipStatus := make(map[int64]bool)
	if printConfig.MinRows > 0 {
		compare = true
	} else if printConfig.MinTime > 0.0 {
		compare = true
	} else if printConfig.IndexMinRows > 0 {
		compare = true
	} else if printConfig.IndexMinTime > 0.0 {
		compare = true
	} else if len(printConfig.Status) > 0 {
		compare = true
		for _, status := range printConfig.Status {
			validStatus[status] = true
		}
	} else if len(printConfig.StatusSkip) > 0 {
		compare = true
		for _, status := range printConfig.StatusSkip {
			skipStatus[status] = true
		}
	}

	var logEntry map[string]interface{}
	queries := make(map[string]*stat.Stat)

	printHeader()
	printFooter()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		err := json.Unmarshal(scanner.Bytes(), &logEntry)
		if err == nil {
			id := stat.LogEntryProcess(logEntry, queries)
			if len(id) > 0 {
				stat := queries[id]

				print := !compare
				if compare {
					if printConfig.MinRows > 0 && printConfig.MinRows <= stat.IndexReadRows+stat.DataReadRows {
						print = true
					} else if printConfig.MinTime > 0.0 && printConfig.MinTime <= stat.RequestTime {
						print = true
					} else if printConfig.IndexMinRows > 0 && printConfig.IndexMinRows <= stat.IndexReadRows {
						print = true
					} else if printConfig.IndexMinTime > 0.0 && printConfig.IndexMinTime <= stat.IndexTime {
						print = true
					} else if len(validStatus) > 0 {
						if _, ok := validStatus[stat.RequestStatus]; ok {
							print = true
						}
					} else if len(skipStatus) > 0 && stat.RequestStatus != 0 {
						if _, ok := skipStatus[stat.RequestStatus]; !ok {
							print = true
						}
					}
				}
				if print {
					printStat(id, stat)
				}

				delete(queries, id)
			}
		}
	}

	for id, s := range queries {
		printStat(id, s)
	}
}

func printFlags(rootCmd *cobra.Command) {
	cmd := &cobra.Command{
		Use:   "print",
		Short: "Read from stdin and print queries stat",
		Run:   printRun,
	}

	cmd.Flags().SortFlags = false

	cmd.Flags().Int64VarP(&printConfig.MinRows, "read_rows", "r", 0, "minimum clickhouse read rows (index + data)")
	cmd.Flags().Float64VarP(&printConfig.MinTime, "time", "t", 0.0, "minimum query time")

	cmd.Flags().Int64VarP(&printConfig.IndexMinRows, "index_read_rows", "i", 0, "minimum clickhouse read rows (index)")
	cmd.Flags().Float64VarP(&printConfig.IndexMinTime, "index_time", "I", 0.0, "minimum query time (index)")

	cmd.Flags().VarP(&printConfig.Status, "status", "s", "responce status")
	cmd.Flags().VarP(&printConfig.StatusSkip, "status-skip", "S", "skip responce status")

	rootCmd.AddCommand(cmd)
}
