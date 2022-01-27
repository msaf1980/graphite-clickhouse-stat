package aggregate

import (
	"fmt"
	"sort"
	"time"

	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/utils"
)

type AggSort int8

const (
	AggSortP99 AggSort = iota
	AggSortP95
	AggSortP90
	AggSortMedian
	AggSortMax
	AggSortSum
)

var aggSortStrings []string = []string{"p99", "p95", "p90", "median", "max", "sum"}

func AggSortStrings() []string {
	return aggSortStrings
}

func (s *AggSort) Set(value string) error {
	switch value {
	case "p99":
		*s = AggSortP99
	case "p95":
		*s = AggSortP95
	case "p90":
		*s = AggSortP90
	case "max":
		*s = AggSortMax
	case "median":
		*s = AggSortMedian
	case "sum":
		*s = AggSortSum
	default:
		return fmt.Errorf("invalid agg sort %s", value)
	}
	return nil
}

func (s *AggSort) String() string {
	return aggSortStrings[*s]
}

func (s *AggSort) Type() string {
	return "agg_sort"
}

type AggNode struct {
	Min    float64
	Max    float64
	Median float64
	P90    float64
	P95    float64
	P99    float64
	Sum    float64
}

func (a *AggNode) Calc(values []float64) error {
	if len(values) == 0 {
		return utils.ErrEmptyInput
	}

	input := make([]float64, len(values))
	copy(input, values)
	sort.Float64s(input)

	var err error

	a.Min = input[0]
	a.Max = input[len(input)-1]
	a.Sum = utils.Sum(input)

	if a.Median, err = utils.Percentile(input, 0.5); err != nil {
		return err
	}
	if a.P90, err = utils.Percentile(input, 0.9); err != nil {
		return err
	}
	if a.P95, err = utils.Percentile(input, 0.95); err != nil {
		return err
	}
	if a.P99, err = utils.Percentile(input, 0.99); err != nil {
		return err
	}

	return nil
}

type StatIndexAggNode struct {
	Key StatIndexKey

	N      int64
	Errors int64

	Metrics AggNode

	IndexReadRows  AggNode
	IndexReadBytes AggNode
	IndexTime      AggNode
}

func LessIndexAggP99ByRows(a, b *StatIndexAggNode) bool {
	if a.IndexReadRows.P99 == b.IndexReadRows.P99 {
		return a.IndexReadRows.Max < b.IndexReadRows.Max
	}
	return a.IndexReadRows.P99 < b.IndexReadRows.P99
}

func LessIndexAggP95ByRows(a, b *StatIndexAggNode) bool {
	if a.IndexReadRows.P95 == b.IndexReadRows.P95 {
		return a.IndexReadRows.Max < b.IndexReadRows.Max
	}
	return a.IndexReadRows.P95 < b.IndexReadRows.P95
}

func LessIndexAggP90ByRows(a, b *StatIndexAggNode) bool {
	if a.IndexReadRows.P90 == b.IndexReadRows.P90 {
		return a.IndexReadRows.Max < b.IndexReadRows.Max
	}
	return a.IndexReadRows.P90 < b.IndexReadRows.P90
}

func LessIndexAggMedianByRows(a, b *StatIndexAggNode) bool {
	if a.IndexReadRows.Median == b.IndexReadRows.Median {
		return a.IndexReadRows.Max < b.IndexReadRows.Max
	}
	return a.IndexReadRows.Median < b.IndexReadRows.Median
}

func LessIndexAggMaxByRows(a, b *StatIndexAggNode) bool {
	if a.IndexReadRows.Max == b.IndexReadRows.Max {
		return a.IndexReadRows.P95 < b.IndexReadRows.P95
	}
	return a.IndexReadRows.Max < b.IndexReadRows.Max
}

func LessIndexAggSumByRows(a, b *StatIndexAggNode) bool {
	if a.IndexReadRows.Sum == b.IndexReadRows.Sum {
		return a.IndexReadRows.P95 < b.IndexReadRows.P95
	}
	return a.IndexReadRows.Sum < b.IndexReadRows.Sum
}

func LessIndexAggSumByErrors(a, b *StatIndexAggNode) bool {
	aErrors := int(1000.0 * float64(a.Errors) / float64(a.N))
	bErrors := int(1000.0 * float64(b.Errors) / float64(b.N))
	if aErrors == bErrors {
		if a.Errors == b.Errors {
			return a.IndexTime.P95 < b.IndexTime.P95
		}
		return a.Errors < b.Errors
	}
	return aErrors < bErrors
}

type StatIndexNode struct {
	N      int64
	Errors int64

	Ids []string

	Metrics []float64

	RequestErrors map[int64]map[string]int64

	IndexReadRows  []float64
	IndexReadBytes []float64
	IndexTime      []float64

	IndexQueryIds []string
}

type StatIndexKey struct {
	Target   string
	Duration time.Duration

	IndexTable string

	RequestType string
}

type StatIndexSummary map[StatIndexKey]*StatIndexNode

func NewStatIndexSummary() StatIndexSummary {
	return make(StatIndexSummary)
}

func (sSum StatIndexSummary) Append(s *stat.Stat) {
	if s.IndexStatus == stat.StatusNone || s.IndexStatus == stat.StatusCached {
		return
	}

	duration := (time.Duration(s.Until-s.From) * time.Second).Truncate(time.Minute)
	key := StatIndexKey{Target: s.Target, Duration: duration, IndexTable: s.IndexTable, RequestType: s.RequestType}
	sNode, ok := sSum[key]
	if !ok {
		sNode = &StatIndexNode{}
		sNode.RequestErrors = make(map[int64]map[string]int64)
		sSum[key] = sNode
	}

	sNode.N++

	sNode.Ids = append(sNode.Ids, s.Id)

	if s.IndexStatus == stat.StatusError {
		sNode.Errors++
		errorMap, ok := sNode.RequestErrors[s.RequestStatus]
		if !ok {
			errorMap = make(map[string]int64)
			sNode.RequestErrors[s.RequestStatus] = errorMap
		}
		errorMap[s.Error]++
	}

	sNode.IndexQueryIds = append(sNode.IndexQueryIds, s.IndexQueryId)

	if s.IndexReadRows > 0 || s.Metrics > 0 {
		sNode.Metrics = append(sNode.Metrics, float64(s.Metrics))

		sNode.IndexReadRows = append(sNode.IndexReadRows, float64(s.IndexReadRows))
		sNode.IndexReadBytes = append(sNode.IndexReadBytes, float64(s.IndexReadBytes))
	}

	sNode.IndexTime = append(sNode.IndexTime, s.IndexTime)
}

func (sSum StatIndexSummary) Aggregate() []StatIndexAggNode {
	aggStat := make([]StatIndexAggNode, len(sSum))

	var i int
	for k, statNode := range sSum {
		aggStat[i].Key = k

		aggStat[i].N = statNode.N
		aggStat[i].Errors = statNode.Errors

		aggStat[i].Metrics.Calc(statNode.Metrics)

		aggStat[i].IndexReadRows.Calc(statNode.IndexReadRows)
		aggStat[i].IndexReadBytes.Calc(statNode.IndexReadBytes)
		aggStat[i].IndexTime.Calc(statNode.IndexTime)

		i++
	}

	return aggStat
}

type StatDataAggNode struct {
	Key StatDataKey

	N      int64
	Errors int64

	Metrics AggNode
	Points  AggNode
	Bytes   AggNode

	DataReadRows  AggNode
	DataReadBytes AggNode
	DataTime      AggNode
}

func LessDataAggP99ByRows(a, b *StatDataAggNode) bool {
	if a.DataReadRows.P99 == b.DataReadRows.P99 {
		return a.DataReadRows.Max < b.DataReadRows.Max
	}
	return a.DataReadRows.P99 < b.DataReadRows.P99
}

func LessDataAggP95ByRows(a, b *StatDataAggNode) bool {
	if a.DataReadRows.P95 == b.DataReadRows.P95 {
		return a.DataReadRows.Max < b.DataReadRows.Max
	}
	return a.DataReadRows.P95 < b.DataReadRows.P95
}

func LessDataAggP90ByRows(a, b *StatDataAggNode) bool {
	if a.DataReadRows.P90 == b.DataReadRows.P90 {
		return a.DataReadRows.Max < b.DataReadRows.Max
	}
	return a.DataReadRows.P90 < b.DataReadRows.P90
}

func LessDataAggMedianByRows(a, b *StatDataAggNode) bool {
	if a.DataReadRows.Median == b.DataReadRows.Median {
		return a.DataReadRows.Max < b.DataReadRows.Max
	}
	return a.DataReadRows.Median < b.DataReadRows.Median
}

func LessDataAggMaxByRows(a, b *StatDataAggNode) bool {
	if a.DataReadRows.Max == b.DataReadRows.Max {
		return a.DataReadRows.P95 < b.DataReadRows.P95
	}
	return a.DataReadRows.Max < b.DataReadRows.Max
}

func LessDataAggSumByRows(a, b *StatDataAggNode) bool {
	if a.DataReadRows.Sum == b.DataReadRows.Sum {
		return a.DataReadRows.P95 < b.DataReadRows.P95
	}
	return a.DataReadRows.Sum < b.DataReadRows.Sum
}

func LessDataAggSumByErrors(a, b *StatDataAggNode) bool {
	aErrors := int(1000.0 * float64(a.Errors) / float64(a.N))
	bErrors := int(1000.0 * float64(b.Errors) / float64(b.N))
	if aErrors == bErrors {
		if a.Errors == b.Errors {
			return a.DataTime.P95 < b.DataTime.P95
		}
		return a.Errors < b.Errors
	}
	return aErrors < bErrors
}

type StatDataNode struct {
	N      int64
	Errors int64

	Ids []string

	RequestStatuses []int64

	Metrics []float64
	Points  []float64
	Bytes   []float64

	RequestTime   []float64
	RequestErrors map[int64]map[string]int64

	IndexCacheHit  int64
	IndexCacheMiss int64

	DataReadRows  []float64
	DataReadBytes []float64
	DataTime      []float64

	DataQueryIds []string
}

type StatDataKey struct {
	Target   string
	Duration time.Duration

	DataTable string

	RequestType string
}

type StatDataSummary map[StatDataKey]*StatDataNode

func NewStatDataSummary() StatDataSummary {
	return make(StatDataSummary)
}

func (sSum StatDataSummary) Append(s *stat.Stat) {
	if s.DataStatus == stat.StatusNone {
		return
	}

	duration := (time.Duration(s.Until-s.From) * time.Second).Truncate(time.Minute)
	key := StatDataKey{Target: s.Target, Duration: duration, DataTable: s.DataTable, RequestType: s.RequestType}
	sNode, ok := sSum[key]
	if !ok {
		sNode = &StatDataNode{}
		sNode.RequestErrors = make(map[int64]map[string]int64)
		sSum[key] = sNode
	}

	sNode.N++

	sNode.RequestStatuses = append(sNode.RequestStatuses, s.RequestStatus)

	sNode.Ids = append(sNode.Ids, s.Id)

	sNode.RequestTime = append(sNode.RequestTime, s.RequestTime)

	if s.IndexStatus == stat.StatusCached {
		sNode.IndexCacheHit++
	} else {
		sNode.IndexCacheMiss++
	}

	if s.DataStatus == stat.StatusError {
		sNode.Errors++
		errorMap, ok := sNode.RequestErrors[s.RequestStatus]
		if !ok {
			errorMap = make(map[string]int64)
			sNode.RequestErrors[s.RequestStatus] = errorMap
		}
		errorMap[s.Error]++
	}

	sNode.DataQueryIds = append(sNode.DataQueryIds, s.DataQueryId)

	if s.DataReadRows > 0 || s.Metrics > 0 {
		sNode.Metrics = append(sNode.Metrics, float64(s.Metrics))
		sNode.Points = append(sNode.Points, float64(s.Points))
		sNode.Bytes = append(sNode.Bytes, float64(s.Bytes))

		sNode.DataReadRows = append(sNode.DataReadRows, float64(s.DataReadRows))
		sNode.DataReadBytes = append(sNode.DataReadBytes, float64(s.DataReadBytes))
		sNode.DataTime = append(sNode.DataTime, s.DataTime)
	}
}

func (sSum StatDataSummary) Aggregate() []StatDataAggNode {
	aggStat := make([]StatDataAggNode, len(sSum))

	var i int
	for k, statNode := range sSum {
		aggStat[i].Key = k

		aggStat[i].N = statNode.N
		aggStat[i].Errors = statNode.Errors

		aggStat[i].Metrics.Calc(statNode.Metrics)
		aggStat[i].Points.Calc(statNode.Points)
		aggStat[i].Bytes.Calc(statNode.Bytes)

		aggStat[i].DataReadRows.Calc(statNode.DataReadRows)
		aggStat[i].DataReadBytes.Calc(statNode.DataReadBytes)
		aggStat[i].DataTime.Calc(statNode.DataTime)

		i++
	}

	return aggStat
}
