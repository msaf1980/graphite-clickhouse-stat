package main

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/msaf1980/go-clipper"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/top"
)

type TopConfig struct {
	Top       int
	Duration  time.Duration
	QuerySort stat.Sort
	Verbose   bool

	File string

	From  time.Time
	Until time.Time
}

var topConfig TopConfig

func printTop(queries map[string]*stat.Stat, n int, sortKey stat.Sort, from, until int64, cleanup bool) {
	stats := top.GetTop(queries, n, sortKey, from, until, cleanup)
	for _, s := range stats {
		printStat(s.Id, s, topConfig.Verbose)
	}
}

func topRun() error {
	if topConfig.Top <= 0 {
		return errors.New("top must be > 0")
	}
	if topConfig.Duration < time.Second {
		return errors.New("flush duration must be >= 1s")
	}

	var timeStamp time.Time

	var (
		in    io.ReadCloser
		err   error
		from  int64
		until int64
	)
	if topConfig.File == "" {
		in = os.Stdin
	} else {
		if in, err = os.Open(topConfig.File); err != nil {
			return err
		}
		defer in.Close()
	}

	queries := make(map[string]*stat.Stat)

	if !topConfig.From.IsZero() {
		from = topConfig.From.UnixNano()
	}
	if !topConfig.Until.IsZero() {
		until = topConfig.Until.UnixNano()
	}

	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		var logEntry map[string]interface{}
		line := scanner.Bytes()
		err := json.Unmarshal(line, &logEntry)
		if err == nil {
			id := stat.LogEntryProcess(logEntry, queries)
			if id != "" {
				s := queries[id]
				print := true
				if from > 0 && s.TimeStamp < from {
					delete(queries, id)
					continue
				}
				if print && until > 0 && s.TimeStamp >= until {
					delete(queries, id)
					continue
				}
				t := time.Unix(s.TimeStamp/1000000000, s.TimeStamp%100000000).Truncate(topConfig.Duration)
				if timeStamp.IsZero() {
					timeStamp = t
				} else if timeStamp != t {
					// next time round, flush  queries
					printHeader(topConfig.Verbose)
					printTop(queries, topConfig.Top, topConfig.QuerySort, from, until, true)
					timeStamp = t
				}
			}
		}
	}

	printTop(queries, topConfig.Top, topConfig.QuerySort, from, until, true)

	return nil
}

func registerTopCmd(registry *clipper.Registry) {
	topCommand, _ := registry.RegisterWithCallback("top", "read from stdin and print top queries stat", topRun)

	topCommand.AddFlag("verbose", "v", &topConfig.Verbose, "verbose")
	topCommand.AddDuration("duration", "d", 10*time.Second, &topConfig.Duration, "flush duration")

	topCommand.AddInt("top", "n", 10, &topConfig.Top, "top queries")
	topCommand.AddValue("sort", "s", &topConfig.QuerySort, false, "top sort by ("+strings.Join(stat.SortStrings(), " | ")+") ")

	topCommand.AddString("input", "i", "", &topConfig.File, "input log file or stdin")

	topCommand.AddTime("from", "f", time.Time{}, &topConfig.From, dateTimeLayout, "start time (UTC)")
	topCommand.AddTime("until", "u", time.Time{}, &topConfig.Until, dateTimeLayout, "end time (UTC)")
}
