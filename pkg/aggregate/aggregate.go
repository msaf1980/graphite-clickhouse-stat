package aggregate

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"

	"github.com/msaf1980/go-stringutils"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/utils"
)

type StatIndexAggNode struct {
	IndexKey StatKey
	// DataKey  StatKey

	Queries []StatQuery

	SampleId string
	ErrorId  string

	N          int64
	ErrorsPcnt float64

	Metrics AggNode

	IndexCacheHitPcnt float64

	ReadRows  AggNode
	ReadBytes AggNode
	Times     AggNode
	// IndexN    AggNode
}

func GreaterIndexAggP99ByTime(a, b *StatIndexAggNode) bool {
	if a.Times.P99 == b.Times.P99 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.Times.P99 > b.Times.P99
}

func GreaterIndexAggP95ByTime(a, b *StatIndexAggNode) bool {
	if a.Times.P95 == b.Times.P95 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.Times.P95 > b.Times.P95
}

func GreaterIndexAggP90ByTime(a, b *StatIndexAggNode) bool {
	if a.Times.P90 == b.Times.P90 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.Times.P90 > b.Times.P90
}

func GreaterIndexAggP50ByTime(a, b *StatIndexAggNode) bool {
	if a.Times.P50 == b.Times.P50 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.Times.P50 > b.Times.P50
}

func GreaterIndexAggMaxByTime(a, b *StatIndexAggNode) bool {
	if a.Times.Max == b.Times.Max {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.Times.Max > b.Times.Max
}

func GreaterIndexAggP99ByRows(a, b *StatIndexAggNode) bool {
	if a.ReadRows.P99 == b.ReadRows.P99 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.ReadRows.P99 > b.ReadRows.P99
}

func GreaterIndexAggP95ByRows(a, b *StatIndexAggNode) bool {
	if a.ReadRows.P95 == b.ReadRows.P95 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.ReadRows.P95 > b.ReadRows.P95
}

func GreaterIndexAggP90ByRows(a, b *StatIndexAggNode) bool {
	if a.ReadRows.P90 == b.ReadRows.P90 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.ReadRows.P90 > b.ReadRows.P90
}

func GreaterIndexAggP50ByRows(a, b *StatIndexAggNode) bool {
	if a.ReadRows.P50 == b.ReadRows.P50 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.ReadRows.P50 > b.ReadRows.P50
}

func GreaterIndexAggMaxByRows(a, b *StatIndexAggNode) bool {
	if a.ReadRows.Max == b.ReadRows.Max {
		return a.ReadRows.P95 > b.ReadRows.P95
	}
	return a.ReadRows.Max > b.ReadRows.Max
}

func GreaterIndexAggByQueries(a, b *StatIndexAggNode) bool {
	if a.N == b.N {
		return a.Times.Max > b.Times.Max
	}
	return a.N > b.N
}

func GreaterIndexAggByErrors(a, b *StatIndexAggNode) bool {
	if a.ErrorsPcnt == b.ErrorsPcnt {
		return a.Times.Max > b.Times.Max
	}
	return a.ErrorsPcnt > b.ErrorsPcnt
}

func SortIndexAgg(statIndexAgg []*StatIndexAggNode, indexSort IndexSort, indexKey AggSortKey) {
	switch indexSort {
	case IndexSortTime:
		switch indexKey {
		case AggSortP99:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexAggP99ByTime(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortP95:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexAggP95ByTime(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortP90:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexAggP90ByTime(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortP50:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexAggP50ByTime(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortMax:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexAggMaxByTime(statIndexAgg[i], statIndexAgg[j])
			})
		default:
			panic(fmt.Errorf("unknown agg sort key: %d", indexKey))
		}
	case IndexSortReadRows:
		switch indexKey {
		case AggSortP99:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexAggP99ByRows(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortP95:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexAggP95ByRows(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortP90:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexAggP90ByRows(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortP50:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexAggP50ByRows(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortMax:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexAggMaxByRows(statIndexAgg[i], statIndexAgg[j])
			})
		default:
			panic(fmt.Errorf("unknown agg sort key: %d", indexKey))
		}
	case IndexSortQueries:
		sort.SliceStable(statIndexAgg, func(i, j int) bool {
			return GreaterIndexAggByQueries(statIndexAgg[i], statIndexAgg[j])
		})
	case IndexSortErrors:
		sort.SliceStable(statIndexAgg, func(i, j int) bool {
			return GreaterIndexAggByErrors(statIndexAgg[i], statIndexAgg[j])
		})
	default:
		panic(fmt.Errorf("unknown agg index sort: %d", indexSort))
	}
}

type LabelKey struct {
	RequestType   string `json:"requestType"`
	DurationLabel string `json:"durationLabel"`
	OffsetLabel   string `json:"OffsetLabel"`
}

func BuildLabelKey(k StatKey) LabelKey {
	return LabelKey{
		RequestType:   k.RequestType,
		DurationLabel: k.DurationLabel,
		OffsetLabel:   k.OffsetLabel,
	}
}

func (k *LabelKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(k)
}

type StatKey struct {
	RequestType   string
	Queries       string
	DurationLabel string
	OffsetLabel   string
}

func (k *StatKey) Empty() bool {
	return k.RequestType == ""
}

type StatQuery struct {
	Query         string
	DurationLabel string
	Offset        string
}

type StatIndexNode struct {
	IndexKey StatKey
	// DataKey  StatKey

	Queries []StatQuery

	SampleId    string
	maxReadRows int64

	ErrorId      string
	maxErrorTime float64

	N      int64
	Errors int64

	Metrics []float64

	IndexCacheHit  int64
	IndexCacheMiss int64

	ReadRows  []float64
	ReadBytes []float64
	Times     []float64
	IndexN    []float64 // TODO: may be refactor with buckets ?
}

type StatIndexSummary map[StatKey]*StatIndexNode

func NewStatIndexSummary() StatIndexSummary {
	return make(StatIndexSummary)
}

func (sSum StatIndexSummary) Append(indexKey StatKey, statIndex []StatQuery, s *stat.Stat) *StatIndexNode {
	sNode, ok := sSum[indexKey]
	if !ok {
		sNode = &StatIndexNode{
			IndexKey:  indexKey,
			Queries:   statIndex,
			ReadRows:  make([]float64, 0, 16),
			ReadBytes: make([]float64, 0, 16),
			Times:     make([]float64, 0, 16),
			Metrics:   make([]float64, 0, 16),
			IndexN:    make([]float64, 0, 16),
		}
		sSum[indexKey] = sNode
	}

	var (
		// cached int64
		errs  int64
		times float64
	)
	for _, q := range s.Index {
		times += q.Time
		switch q.Status {
		case stat.StatusSuccess:
			sNode.IndexCacheMiss++
		case stat.StatusError:
			errs++
		case stat.StatusCached:
			// cached++
			sNode.IndexCacheHit++
		}
	}
	sNode.N++
	if errs == 0 {
		sNode.ReadRows = append(sNode.ReadRows, float64(s.IndexReadRows))
		sNode.ReadBytes = append(sNode.ReadBytes, float64(s.IndexReadBytes))
		sNode.Metrics = append(sNode.Metrics, float64(s.Metrics))
	} else {
		sNode.Errors++
		sNode.ErrorId = s.Id
		if sNode.maxErrorTime < s.QueryTime {
			sNode.maxErrorTime = s.QueryTime
		}
	}
	sNode.Times = append(sNode.Times, times)

	// sNode.ErrorsPcnt = append(sNode.ErrorsPcnt, float64(errs)/float64(len(s.Index))*100)
	sNode.IndexN = append(sNode.IndexN, float64(len(s.Index)))

	if sNode.maxReadRows < s.IndexReadRows {
		sNode.maxReadRows = s.IndexReadRows
		sNode.SampleId = s.Id
	}
	return sNode
}

func (sSum StatIndexSummary) Aggregate() map[LabelKey][]*StatIndexAggNode {
	// aggStat := make([]*StatIndexAggNode, len(sSum))
	aggStats := make(map[LabelKey][]*StatIndexAggNode)

	for _, statNode := range sSum {
		label := BuildLabelKey(statNode.IndexKey)

		aggStat := new(StatIndexAggNode)
		aggStat.IndexKey = statNode.IndexKey
		// aggStat.DataKey = statNode.DataKey

		aggStat.Queries = statNode.Queries

		aggStat.SampleId = statNode.SampleId
		aggStat.ErrorId = statNode.ErrorId

		aggStat.N = statNode.N
		aggStat.ErrorsPcnt = float64(statNode.Errors) / float64(statNode.N) * 100

		_ = aggStat.Metrics.Calc(statNode.Metrics)

		IndexCache := statNode.IndexCacheMiss + statNode.IndexCacheHit
		if IndexCache > 0 {
			aggStat.IndexCacheHitPcnt = float64(statNode.IndexCacheHit) / float64(IndexCache) * 100
		}

		_ = aggStat.ReadRows.Calc(statNode.ReadRows)
		_ = aggStat.ReadBytes.Calc(statNode.ReadBytes)
		_ = aggStat.Times.Calc(statNode.Times)

		// aggStat.IndexN.Calc(statNode.IndexN)

		aggStats[label] = append(aggStats[label], aggStat)
	}

	return aggStats
}

type StatRequestAggNode struct {
	IndexKey StatKey
	DataKey  StatKey

	Queries []StatQuery

	SampleId string
	ErrorId  string

	N                 int64
	ErrorsPcnt        float64
	IndexErrorsPcnt   float64
	IndexCacheHitPcnt float64
	DataErrorsPcnt    float64

	RequestStatus map[int64]int64

	Metrics AggNode
	Points  AggNode
	Bytes   AggNode

	ReadRows     AggNode
	ReadBytes    AggNode
	RequestTimes AggNode
	QueryTimes   AggNode

	DataReadRows  AggNode
	DataReadBytes AggNode
	DataTimes     AggNode

	// DataN AggNode

	IndexReadRows  AggNode
	IndexReadBytes AggNode
	IndexTimes     AggNode
}

func LessDataAggP99ByRows(a, b *StatRequestAggNode) bool {
	if a.ReadRows.P99 == b.ReadRows.P99 {
		return a.ReadRows.Max < b.ReadRows.Max
	}
	return a.ReadRows.P99 < b.ReadRows.P99
}

func LessDataAggP95ByRows(a, b *StatRequestAggNode) bool {
	if a.ReadRows.P95 == b.ReadRows.P95 {
		return a.ReadRows.Max < b.ReadRows.Max
	}
	return a.ReadRows.P95 < b.ReadRows.P95
}

func LessDataAggP90ByRows(a, b *StatRequestAggNode) bool {
	if a.ReadRows.P90 == b.ReadRows.P90 {
		return a.ReadRows.Max < b.ReadRows.Max
	}
	return a.ReadRows.P90 < b.ReadRows.P90
}

func LessDataAggMedianByRows(a, b *StatRequestAggNode) bool {
	if a.ReadRows.P50 == b.ReadRows.P50 {
		return a.ReadRows.Max < b.ReadRows.Max
	}
	return a.ReadRows.P50 < b.ReadRows.P50
}

func LessDataAggMaxByRows(a, b *StatRequestAggNode) bool {
	if a.ReadRows.Max == b.ReadRows.Max {
		return a.ReadRows.P95 < b.ReadRows.P95
	}
	return a.ReadRows.Max < b.ReadRows.Max
}

func LessDataAggSumByErrors(a, b *StatRequestAggNode) bool {
	if a.ErrorsPcnt == b.ErrorsPcnt {
		return a.QueryTimes.Max < b.QueryTimes.Max
	}
	return a.ErrorsPcnt < b.ErrorsPcnt
}

type StatQueryNode struct {
	IndexKey StatKey
	DataKey  StatKey

	Queries []StatQuery

	SampleId    string
	maxReadRows int64

	ErrorId      string
	maxErrorTime float64

	N           int64
	Errors      int64
	IndexErrors int64
	DataErrors  int64

	IndexCacheHit  int64
	IndexCacheMiss int64

	RequestStatus map[int64]int64
	RequestTimes  []float64
	QueryTimes    []float64

	Metrics []float64
	Points  []float64
	Bytes   []float64

	ReadRows  []float64
	ReadBytes []float64

	DataReadRows  []float64
	DataReadBytes []float64
	DataTimes     []float64

	IndexReadRows  []float64
	IndexReadBytes []float64
	IndexTimes     []float64

	// DataErrorsPcnt []float64
	// DataN          []float64
}

type StatRequestSummary map[StatKey]*StatQueryNode

func NewStatQuerySummary() StatRequestSummary {
	return make(StatRequestSummary)
}

func (sSum StatRequestSummary) Append(indexKey, dataKey StatKey, statQueries []StatQuery, s *stat.Stat) *StatQueryNode {
	sNode, ok := sSum[dataKey]
	if !ok {
		sNode = &StatQueryNode{
			IndexKey:       indexKey,
			DataKey:        dataKey,
			Queries:        statQueries,
			RequestStatus:  make(map[int64]int64),
			RequestTimes:   make([]float64, 0, 16),
			QueryTimes:     make([]float64, 0, 16),
			ReadRows:       make([]float64, 0, 16),
			ReadBytes:      make([]float64, 0, 16),
			DataReadBytes:  make([]float64, 0, 16),
			DataReadRows:   make([]float64, 0, 16),
			DataTimes:      make([]float64, 0, 16),
			IndexReadBytes: make([]float64, 0, 16),
			IndexReadRows:  make([]float64, 0, 16),
			IndexTimes:     make([]float64, 0, 16),
			Metrics:        make([]float64, 0, 16),
			Points:         make([]float64, 0, 16),
			// DataErrorsPcnt: make([]float64, 0, 16),
			// DataN:          make([]float64, 0, 16),
		}
		sSum[dataKey] = sNode
	}

	var (
		dataErrs, indexErrs   int64
		dataTimes, indexTimes float64
	)
	sNode.N++
	sNode.RequestStatus[s.RequestStatus]++
	if s.RequestStatus == http.StatusOK || s.RequestStatus == http.StatusNotFound {
		sNode.ReadRows = append(sNode.ReadRows, float64(s.ReadRows))
		sNode.ReadBytes = append(sNode.ReadBytes, float64(s.ReadBytes))
	} else {
		sNode.Errors++
		if sNode.maxErrorTime < s.QueryTime {
			sNode.maxErrorTime = s.QueryTime
			sNode.ErrorId = s.Id
		}
	}
	sNode.RequestTimes = append(sNode.RequestTimes, s.RequestTime)
	sNode.QueryTimes = append(sNode.QueryTimes, s.QueryTime)

	if len(s.Index) > 0 {
		for _, idx := range s.Index {
			indexTimes += idx.Time
			switch idx.Status {
			case stat.StatusSuccess:
				sNode.IndexCacheMiss++
			case stat.StatusCached:
				sNode.IndexCacheHit++
			case stat.StatusError:
				indexErrs++
			}
		}

		sNode.IndexTimes = append(sNode.IndexTimes, indexTimes)

		if indexErrs == 0 {
			sNode.Metrics = append(sNode.Metrics, float64(s.Metrics))
			sNode.IndexReadRows = append(sNode.IndexReadRows, float64(s.IndexReadRows))
			sNode.IndexReadBytes = append(sNode.IndexReadBytes, float64(s.IndexReadBytes))
		} else {
			sNode.IndexErrors++
		}
	}

	if len(s.Data) > 0 {
		for _, q := range s.Data {
			dataTimes += q.Time
			switch q.Status {
			case stat.StatusError:
				dataErrs++
			}
		}

		sNode.DataTimes = append(sNode.DataTimes, dataTimes)

		// sNode.DataErrorsPcnt = append(sNode.DataErrorsPcnt, float64(dataErrs)/float64(len(s.Data))*100)
		// sNode.DataN = append(sNode.DataN, float64(len(s.Data)))

		if dataErrs == 0 {
			sNode.Points = append(sNode.Points, float64(s.Points))
			sNode.DataReadRows = append(sNode.DataReadRows, float64(s.DataReadRows))
			sNode.DataReadBytes = append(sNode.DataReadBytes, float64(s.DataReadBytes))
		} else {
			sNode.DataErrors++
		}
	}

	if sNode.maxReadRows < s.ReadRows {
		sNode.maxReadRows = s.ReadRows
		sNode.SampleId = s.Id
	}

	return sNode
}

func (sSum StatRequestSummary) Aggregate() map[LabelKey][]*StatRequestAggNode {
	aggStats := make(map[LabelKey][]*StatRequestAggNode)

	for _, statNode := range sSum {
		label := BuildLabelKey(statNode.DataKey)

		aggStat := new(StatRequestAggNode)
		aggStat.IndexKey = statNode.IndexKey
		aggStat.DataKey = statNode.DataKey

		aggStat.Queries = statNode.Queries

		aggStat.RequestStatus = statNode.RequestStatus

		aggStat.N = statNode.N
		aggStat.ErrorsPcnt = float64(statNode.Errors) / float64(statNode.N) * 100
		aggStat.DataErrorsPcnt = float64(statNode.DataErrors) / float64(statNode.N) * 100
		aggStat.IndexErrorsPcnt = float64(statNode.IndexErrors) / float64(statNode.N) * 100

		IndexCache := statNode.IndexCacheMiss + statNode.IndexCacheHit
		if IndexCache > 0 {
			aggStat.IndexCacheHitPcnt = float64(statNode.IndexCacheHit) / float64(IndexCache) * 100
		}

		aggStat.SampleId = statNode.SampleId
		aggStat.ErrorId = statNode.ErrorId

		_ = aggStat.Metrics.Calc(statNode.Metrics)
		_ = aggStat.Points.Calc(statNode.Points)
		_ = aggStat.Bytes.Calc(statNode.Bytes)

		_ = aggStat.ReadRows.Calc(statNode.ReadRows)
		_ = aggStat.ReadBytes.Calc(statNode.ReadBytes)
		_ = aggStat.RequestTimes.Calc(statNode.RequestTimes)
		_ = aggStat.QueryTimes.Calc(statNode.QueryTimes)

		_ = aggStat.DataReadRows.Calc(statNode.DataReadRows)
		_ = aggStat.DataReadBytes.Calc(statNode.DataReadBytes)
		_ = aggStat.DataTimes.Calc(statNode.DataTimes)

		aggStats[label] = append(aggStats[label], aggStat)
	}

	return aggStats
}

func BuildStatKey(s *stat.Stat) (indexKey, queryKey *StatKey, statIndex, statQueries []StatQuery) {
	var (
		sbIndex, sbQuery stringutils.Builder
		maxDays          int
		maxDaysStr       string
		minOffset        int64
		minOffsetStr     string
		maxDuration      int64
		maxDurationStr   string
	)
	statQueries = make([]StatQuery, 0, len(s.Queries))
	statIndex = make([]StatQuery, 0, len(s.Queries))

	sbIndex.Grow(128)
	_ = sbIndex.WriteByte('[')
	indexKey = &StatKey{RequestType: s.RequestType}

	queryKey = &StatKey{RequestType: s.RequestType}
	sbQuery.Grow(128)
	_ = sbQuery.WriteByte('[')

	for _, q := range s.Queries {
		_, _ = sbIndex.WriteString("{query='")
		_, _ = sbIndex.WriteString(q.Query)
		_ = sbIndex.WriteByte('\'')

		var (
			daysStr                string
			durationStr, offsetStr string
		)
		if q.Days != 0 {
			daysStr = utils.FormatDuration(int64(q.Days)*86400, false)
			if maxDays < q.Days {
				maxDays = q.Days
				maxDaysStr = daysStr
			}
			_, _ = sbIndex.WriteString(",index=")
			_, _ = sbIndex.WriteString(daysStr)
		}

		_, _ = sbQuery.WriteString("{query='")
		_, _ = sbQuery.WriteString(q.Query)
		_ = sbQuery.WriteByte('\'')

		if q.From != 0 && q.Until != 0 {
			duration := q.Until - q.From
			if duration != 0 {
				if duration < 0 {
					duration = -duration
				}
				durationStr = utils.FormatDuration(duration, false)
				if maxDuration < duration {
					maxDuration = duration
					maxDurationStr = durationStr
				}
				_, _ = sbQuery.WriteString(",render=")
				_, _ = sbQuery.WriteString(durationStr)
			}
			offset := s.TimeStamp/1e9 - q.Until
			if offset > 0 {
				if offsetStr = utils.FormatDuration(offset, true); offsetStr != "" {
					if (minOffset == math.MaxInt64 && minOffsetStr == "") || offset < minOffset {
						minOffset = offset
						minOffsetStr = offsetStr
					}
					_, _ = sbQuery.WriteString(",offset=")
					_, _ = sbQuery.WriteString(offsetStr)
				}
			}
		}

		_ = sbIndex.WriteByte('}')
		_ = sbQuery.WriteByte('}')

		statIndex = append(statIndex, StatQuery{
			Query:         q.Query,
			DurationLabel: daysStr,
		})
		statQueries = append(statQueries, StatQuery{
			Query:         q.Query,
			DurationLabel: durationStr,
			Offset:        offsetStr,
		})
	}
	_ = sbIndex.WriteByte(']')
	indexKey.Queries = sbIndex.String()
	indexKey.DurationLabel = maxDaysStr

	_ = sbQuery.WriteByte(']')
	queryKey.Queries = sbQuery.String()
	queryKey.DurationLabel = maxDurationStr
	if !(minOffset == math.MaxInt64 && minOffsetStr == "") {
		queryKey.OffsetLabel = minOffsetStr
	}

	return
}

type StatAggSumSlice struct {
	Index    []*StatIndexAggNode
	Requests []*StatRequestAggNode
}

type StatAggSum struct {
	Index map[LabelKey][]*StatIndexAggNode
	// DataIndex map[StatKey]*StatIndexAggNode
	Requests map[LabelKey][]*StatRequestAggNode
}

func LabelsSort(keys []LabelKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].RequestType == keys[j].RequestType {
			if keys[i].DurationLabel == keys[j].DurationLabel {
				return keys[i].OffsetLabel < keys[j].OffsetLabel
			}
			return keys[i].DurationLabel < keys[j].DurationLabel
		}
		return keys[i].RequestType < keys[j].RequestType
	})
}

func (aggSum *StatAggSum) IndexLabels() []LabelKey {
	keys := make([]LabelKey, len(aggSum.Index))
	for k := range aggSum.Index {
		keys = append(keys, k)
	}
	LabelsSort(keys)

	return keys
}

func (aggSum *StatAggSum) RequestLabels() []LabelKey {
	keys := make([]LabelKey, len(aggSum.Requests))
	for k := range aggSum.Requests {
		keys = append(keys, k)
	}
	LabelsSort(keys)

	return keys
}
func (aSum *StatAggSum) Slice() StatAggSumSlice {
	agg := StatAggSumSlice{
		Index:    make([]*StatIndexAggNode, 0, len(aSum.Index)*2),
		Requests: make([]*StatRequestAggNode, 0, len(aSum.Requests)*2),
	}
	for _, s := range aSum.Index {
		agg.Index = append(agg.Index, s...)
	}
	for _, s := range aSum.Requests {
		agg.Requests = append(agg.Requests, s...)
	}

	return agg
}

func NewAggSummary() *StatAggSum {
	return &StatAggSum{
		Index:    make(map[LabelKey][]*StatIndexAggNode),
		Requests: make(map[LabelKey][]*StatRequestAggNode),
	}
}

type StatSummary struct {
	Index StatIndexSummary
	// DataIndex StatIndexSummary
	Requests StatRequestSummary
}

func NewStatSummary() *StatSummary {
	return &StatSummary{
		Index: NewStatIndexSummary(),
		// DataIndex: NewStatIndexSummary(),
		Requests: NewStatQuerySummary(),
	}
}

func (sSum *StatSummary) Append(s *stat.Stat) {
	indexKey, dataKey, statIndex, statQueries := BuildStatKey(s)

	// idx := sSum.Index.Append(*indexKey, statIndex, s)
	// if dataKey != nil {
	// key := *dataKey
	// idx.DataKey = key
	// sSum.DataIndex[key] = idx
	// sSum.Queries.Append(*dataKey, statQueries, s)
	// }

	sSum.Index.Append(*indexKey, statIndex, s)
	sSum.Requests.Append(*indexKey, *dataKey, statQueries, s)
}

func (sSum *StatSummary) Aggregate() *StatAggSum {
	statAggSum := &StatAggSum{}
	statAggSum.Index = sSum.Index.Aggregate()
	statAggSum.Requests = sSum.Requests.Aggregate()
	// for _, labels := range statAggSum.Index {
	// 	for _, idx := range labels {
	// 		if !idx.DataKey.Empty() {
	// 			statAggSum.DataIndex[idx.DataKey] = idx
	// 		}
	// 	}
	// }

	return statAggSum
}
