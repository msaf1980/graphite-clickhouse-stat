package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/goccy/go-json"

	"github.com/msaf1980/go-clipper"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/utils"
)

type PrintConfig struct {
	MinRows      int64
	MinTime      float64
	IndexMinRows int64
	DataMinRows  int64
	// IndexMinTime float64
	Status     []int64
	StatusSkip []int64
	Verbose    bool

	From  time.Time
	Until time.Time

	File string
}

var printConfig PrintConfig

var (
	footerPrint      = headLine(204, '-')
	labelFooterPrint = headLine(204, '=')
)

func printFooter() {
	fmt.Println(footerPrint)
}

func printLabelFooter() {
	fmt.Println(labelFooterPrint)
}

func printHeader(verbose bool) {
	printFooter()
	fmt.Printf("%19s | %3s | %10s | %10s | %10s |%s| %10s | %10s | %16s | %32s | %7s | %8s | %8s | %10s | %s\n",
		"timestamp (UTC)", "S", "rtime", "wtime", "qtime", "W",
		"read_rows", "read_bytes",
		"type", "request_id", "metrics", "points", "size",
		"iread_rows", "dread_rows",
	)
	if verbose {
		printFooter()
		fmt.Printf("%19s | %3s | %10s | %10s | %s\n",
			"index days", "", "duration", "offset", "query",
		)

		printFooter()
		// index/data stat
		fmt.Printf("%19s | %3s | %10s | %10s | %10s |%s| %10s | %10s | %51s | %-29s | %s\n",
			"index_days", "", "duration", "offset", "time", "S", "read_rows", "read_bytes", "query_id", "table", "error",
		)
	}
	printFooter()
}

func headLine(n int, c byte) string {
	out := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, c)
	}
	return string(out)
}

func printStat(id string, s *stat.Stat, verbose bool) {

	fmt.Printf("%19s | %3d | %10.2f | %10.2f | %10.2f |%s| %10s | %10s | %16s | %32s"+ // last - id
		" | %7s | %8s | %8s | %10s | %s\n", // metrics, points, bytes, read_rows, read_bytes
		time.Unix(s.TimeStamp/1e9, 0).UTC().Format("2006-01-02 15:04:05"),
		s.RequestStatus,
		s.RequestTime, s.WaitTime, s.QueryTime, s.WaitStatus.String(),
		utils.FormatNumber(s.ReadRows), utils.FormatBytes(s.ReadBytes),
		s.RequestType, id,
		utils.FormatNumber(s.Metrics), utils.FormatNumber(s.Points), utils.FormatBytes(s.Bytes),
		utils.FormatNumber(s.IndexReadRows), utils.FormatNumber(s.DataReadRows),
	)
	if verbose {
		for _, q := range s.Queries {
			var d, offset string
			if q.From > 0 && q.Until > 0 {
				d = utils.FormatTruncSeconds(q.Until - q.From)
				offset = utils.FormatTruncSeconds(s.TimeStamp/1e9 - q.Until)
			}
			fmt.Printf("%19s | %3s | %10s | %10s | %s\n",
				utils.FormatInt(q.Days), "", d, offset, q.Query,
			)
		}

		// index stat
		for _, q := range s.Index {
			fmt.Printf("%19s | %3s | %10s | %10s | %10.2f |%s| %10s | %10s | %51s | %-29s | %s\n",
				utils.FormatInt(q.Days), "", "", "",
				q.Time, q.Status.String(),
				utils.FormatNumber(q.ReadRows), utils.FormatBytes(q.ReadBytes), q.QueryId, q.Table, q.Error,
			)
		}
		// data stat
		for _, q := range s.Data {
			var d, offset string
			if q.From > 0 && q.Until > 0 {
				d = utils.FormatTruncSeconds(q.Until - q.From)
				offset = utils.FormatTruncSeconds(s.TimeStamp/1e9 - q.Until)
			}
			fmt.Printf("%19s | %3s | %10s | %10s | %10.2f |%s| %10s | %10s | %51s | %s\n",
				utils.FormatInt(q.Days), "", d, offset,
				q.Time, q.Status.String(),
				utils.FormatNumber(q.ReadRows), utils.FormatBytes(q.ReadBytes), q.QueryId, q.Table,
			)
		}
	}
}

func printRun() error {
	if len(printConfig.Status) > 0 && len(printConfig.StatusSkip) > 0 {
		return errors.New("status and status-skip can't be coexist")
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
		// } else if printConfig.DataMinTime > 0.0 {
		// 	compare = true
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

	var (
		in    io.ReadCloser
		err   error
		from  int64
		until int64
	)
	if printConfig.File == "" {
		in = os.Stdin
	} else {
		if in, err = os.Open(printConfig.File); err != nil {
			return err
		}
		defer in.Close()
	}

	if !printConfig.From.IsZero() {
		from = printConfig.From.UnixNano()
	}
	if !printConfig.Until.IsZero() {
		until = printConfig.Until.UnixNano()
	}

	queries := make(map[string]*stat.Stat)

	printHeader(printConfig.Verbose)

	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		var logEntry map[string]interface{}
		line := scanner.Bytes()
		err := json.Unmarshal(line, &logEntry)
		if err == nil {
			id := stat.LogEntryProcess(logEntry, queries)
			if id != "" {
				stat := queries[id]

				print := true
				if from > 0 && stat.TimeStamp < from {
					print = false
				}
				if print && until > 0 && stat.TimeStamp >= until {
					print = false
				}

				if print && compare {
					if printConfig.MinRows > 0 && stat.ReadRows <= printConfig.MinRows {
						print = false
					} else if printConfig.MinTime > 0.0 && stat.RequestTime <= printConfig.MinTime {
						print = false
					} else if printConfig.IndexMinRows > 0 && stat.IndexReadRows < printConfig.IndexMinRows {
						print = false
					} else if printConfig.DataMinRows > 0 && stat.DataReadRows < printConfig.DataMinRows {
						print = false
					} else if len(validStatus) > 0 {
						if _, ok := validStatus[stat.RequestStatus]; ok {
							print = false
						}
					} else if len(skipStatus) > 0 {
						if _, ok := skipStatus[stat.RequestStatus]; !ok {
							print = false
						}
					}
				}
				if print {
					printStat(id, stat, printConfig.Verbose)
				}

				delete(queries, id)
			}
		}
	}

	return nil
}

var dateTimeLayout = "2006-01-02T15:04:05"

func registerPrintCmd(registry *clipper.Registry) {
	printCommand, _ := registry.RegisterWithCallback("print", "read and print queries stat", printRun)

	printCommand.AddFloat64("time", "t", 0.0, &printConfig.MinTime, "minimum query time")
	printCommand.AddInt64N("read_rows", "r", 0, &printConfig.MinRows, "minimum clickhouse read rows (index + data)")

	printCommand.AddInt64N("i_read_rows", "I", 0, &printConfig.IndexMinRows, "minimum clickhouse read rows (index)")
	printCommand.AddInt64N("d_read_rows", "D", 0, &printConfig.DataMinRows, "minimum clickhouse read rows (data)")
	// cmd.Flags().Float64VarP(&printConfig.IndexMinTime, "index_time", "I", 0.0, "minimum query time (index)")

	printCommand.AddInt64Array("status", "s", []int64{}, &printConfig.Status, "responce status")
	printCommand.AddInt64Array("status-skip", "S", []int64{}, &printConfig.StatusSkip, "skip responce status")

	printCommand.AddFlag("verbose", "v", &printConfig.Verbose, "verbose")

	printCommand.AddString("input", "i", "", &printConfig.File, "input log file or stdin")

	printCommand.AddTime("from", "f", time.Time{}, &printConfig.From, dateTimeLayout, "start time (UTC)")
	printCommand.AddTime("until", "u", time.Time{}, &printConfig.Until, dateTimeLayout, "end time (UTC)")
}
