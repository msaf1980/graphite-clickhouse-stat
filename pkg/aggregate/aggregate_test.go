package aggregate

import (
	"reflect"
	"testing"
	"time"

	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
)

func Test_statSummary_append(t *testing.T) {
	stats := []*stat.Stat{
		{
			Id:     "1",
			Target: "test.*",

			Metrics: 10,
			Points:  20,
			Bytes:   110000,

			From:  0,
			Until: int64(time.Minute / time.Second),

			RequestType:   "render",
			RequestTime:   0.3,
			RequestStatus: 200,

			IndexStatus:    stat.StatusSuccess,
			IndexReadRows:  1000,
			IndexReadBytes: 10000,
			IndexTime:      0.1,
			IndexTable:     "graphite_index",
			IndexQueryId:   "Index_1",

			DataStatus:    stat.StatusSuccess,
			DataReadRows:  10000,
			DataReadBytes: 100000,
			DataTime:      0.2,
			DataTable:     "graphite",
			DataQueryId:   "points_1",
		},
		{
			Id:     "2",
			Target: "test.*",

			Metrics: 10,
			Points:  20,
			Bytes:   90000,

			From:  int64(time.Minute / time.Second),
			Until: int64(time.Minute/time.Second) * 2,

			RequestType:   "render",
			RequestTime:   0.1,
			RequestStatus: 200,

			IndexStatus: stat.StatusCached,

			DataStatus:    stat.StatusSuccess,
			DataReadRows:  9000,
			DataReadBytes: 90000,
			DataTime:      0.1,
			DataTable:     "graphite",
			DataQueryId:   "points_2",
		},
		{
			Id:     "3",
			Target: "test.*",

			Metrics: 10,
			Points:  200,
			Bytes:   900000,

			From:  0,
			Until: int64(time.Minute/time.Second) * 10,

			RequestType:   "render",
			RequestTime:   1.0,
			RequestStatus: 200,

			IndexStatus: stat.StatusCached,

			DataStatus:    stat.StatusSuccess,
			DataReadRows:  100000,
			DataReadBytes: 1000000,
			DataTime:      2,
			DataTable:     "graphite_hist",
			DataQueryId:   "points_3",
		},
	}

	wantStatIndexSum := StatIndexSummary{
		{Target: "test.*", Duration: time.Minute, IndexTable: "graphite_index", RequestType: "render"}: &StatIndexNode{
			N: 1,

			Ids: []string{"1"},

			Metrics: []float64{10},

			RequestStatus: map[int64]int64{200: 1},

			IndexReadRows:  []float64{1000},
			IndexReadBytes: []float64{10000},
			IndexTime:      []float64{0.1},

			IndexQueryIds: []string{"Index_1"},
		},
	}

	wantStatIndexAgg := []StatIndexAggNode{
		{
			Key:            StatIndexKey{Target: "test.*", Duration: time.Minute, IndexTable: "graphite_index", RequestType: "render"},
			Metrics:        AggNode{Min: 10, Max: 10, Median: 10, P90: 10, P95: 10, P99: 10, Sum: 10},
			IndexReadRows:  AggNode{Min: 1000, Max: 1000, Median: 1000, P90: 1000, P95: 1000, P99: 1000, Sum: 1000},
			IndexReadBytes: AggNode{Min: 10000, Max: 10000, Median: 10000, P90: 10000, P95: 10000, P99: 10000, Sum: 10000},
			IndexTime:      AggNode{Min: 0.1, Max: 0.1, Median: 0.1, P90: 0.1, P95: 0.1, P99: 0.1, Sum: 0.1},
		},
	}

	wantStatDataSum := StatDataSummary{
		{Target: "test.*", Duration: time.Minute, DataTable: "graphite", RequestType: "render"}: &StatDataNode{
			N: 2,

			Ids: []string{"1", "2"},

			Metrics: []float64{10, 10},
			Points:  []float64{20, 20},
			Bytes:   []float64{110000, 90000},

			RequestTime:   []float64{0.3, 0.1},
			RequestStatus: map[int64]int64{200: 2},

			IndexCacheHit:  1,
			IndexCacheMiss: 1,

			DataReadRows:  []float64{10000, 9000},
			DataReadBytes: []float64{100000, 90000},
			DataTime:      []float64{0.2, 0.1},

			DataQueryIds: []string{"points_1", "points_2"},
		},
		{Target: "test.*", Duration: time.Minute * 10, DataTable: "graphite_hist", RequestType: "render"}: &StatDataNode{
			N: 1,

			Ids: []string{"3"},

			Metrics: []float64{10},
			Points:  []float64{200},
			Bytes:   []float64{900000},

			IndexCacheHit:  1,
			IndexCacheMiss: 0,

			RequestTime:   []float64{1.0},
			RequestStatus: map[int64]int64{200: 1},

			DataReadRows:  []float64{100000},
			DataReadBytes: []float64{1000000},
			DataTime:      []float64{2.0},

			DataQueryIds: []string{"points_3"},
		},
	}

	wantStatDataAgg := []StatDataAggNode{
		{
			Key:           StatDataKey{Target: "test.*", Duration: time.Minute, DataTable: "graphite", RequestType: "render"},
			Metrics:       AggNode{Min: 10, Max: 10, Median: 10, P90: 10, P95: 10, P99: 10, Sum: 20},
			Points:        AggNode{Min: 20, Max: 20, Median: 20, P90: 20, P95: 20, P99: 20, Sum: 40},
			Bytes:         AggNode{Min: 90000, Max: 110000, Median: 90000, P90: 100000, P95: 100000, P99: 100000, Sum: 90000 + 110000},
			DataReadRows:  AggNode{Min: 9000, Max: 10000, Median: 9000, P90: 9500, P95: 9500, P99: 9500, Sum: 9000 + 10000},
			DataReadBytes: AggNode{Min: 90000, Max: 100000, Median: 90000, P90: 95000, P95: 95000, P99: 95000, Sum: 90000 + 100000},
			DataTime:      AggNode{Min: 0.1, Max: 0.2, Median: 0.1, P90: 0.15000000000000002, P95: 0.15000000000000002, P99: 0.15000000000000002, Sum: 0.30000000000000004},
		},
		{
			Key:           StatDataKey{Target: "test.*", Duration: time.Minute * 10, DataTable: "graphite_hist", RequestType: "render"},
			Metrics:       AggNode{Min: 10, Max: 10, Median: 10, P90: 10, P95: 10, P99: 10, Sum: 10},
			Points:        AggNode{Min: 200, Max: 200, Median: 200, P90: 200, P95: 200, P99: 200, Sum: 200},
			Bytes:         AggNode{Min: 900000, Max: 900000, Median: 900000, P90: 900000, P95: 900000, P99: 900000, Sum: 900000},
			DataReadRows:  AggNode{Min: 100000, Max: 100000, Median: 100000, P90: 100000, P95: 100000, P99: 100000, Sum: 100000},
			DataReadBytes: AggNode{Min: 1000000, Max: 1000000, Median: 1000000, P90: 1000000, P95: 1000000, P99: 1000000, Sum: 1000000},
			DataTime:      AggNode{Min: 2.0, Max: 2.0, Median: 2.0, P90: 2.0, P95: 2.0, P99: 2.0, Sum: 2.0},
		},
	}

	statIndexSum := NewStatIndexSummary()
	statDataSum := NewStatDataSummary()

	for _, s := range stats {
		statIndexSum.Append(s)
		statDataSum.Append(s)
	}

	if reflect.DeepEqual(statIndexSum, wantStatIndexSum) {
		statIndexAgg := statIndexSum.Aggregate()

		// print Index agg stat diff
		maxLen := len(statIndexAgg)
		if maxLen < len(wantStatIndexAgg) {
			maxLen = len(wantStatIndexAgg)
		}
		for i := 0; i < maxLen; i++ {
			if i < len(statIndexAgg) && i < len(wantStatIndexAgg) {
				if statIndexAgg[i] != wantStatIndexAgg[i] {
					t.Errorf("\n- agg index[%d] = %+v\n+ agg index[%d] = %+v\n", i, wantStatIndexAgg[i], i, statIndexAgg[i])
				}
			} else if i >= len(statIndexAgg) {
				t.Errorf("\n- agg index[%d] = %+v\n", i, wantStatIndexAgg[i])
			} else if maxLen >= len(wantStatIndexAgg) {
				t.Errorf("\n+ agg index[%d] = %+v\n", i, statIndexAgg[i])
			}
		}
	} else {
		// print Index stat diff
		for k, sNode := range statIndexSum {
			if wNode, ok := wantStatIndexSum[k]; ok {
				if !reflect.DeepEqual(*sNode, *wNode) {
					t.Errorf("\n- index[%+v] = %+v\n+ index[%+v] = %+v\n", k, *wNode, k, *sNode)
				}
			} else {
				t.Errorf("\n+ index[%+v] = %+v\n", k, *sNode)
			}
		}
		for k, wNode := range wantStatIndexSum {
			if _, ok := statIndexSum[k]; !ok {
				t.Errorf("\n- index[%+v] = %+v\n", k, *wNode)
			}
		}
	}

	if reflect.DeepEqual(statDataSum, wantStatDataSum) {
		statDataAgg := statDataSum.Aggregate()

		// print Data agg stat diff
		maxLen := len(statDataAgg)
		if maxLen < len(wantStatDataAgg) {
			maxLen = len(wantStatDataAgg)
		}
		for i := 0; i < maxLen; i++ {
			if i < len(statDataAgg) && i < len(wantStatDataAgg) {
				if statDataAgg[i] != wantStatDataAgg[i] {
					t.Errorf("\n- agg data[%d] = %+v\n+ agg data[%d] = %+v\n", i, wantStatDataAgg[i], i, statDataAgg[i])
				}
			} else if i >= len(statDataAgg) {
				t.Errorf("\n- agg data[%d] = %+v\n", i, wantStatDataAgg[i])
			} else if maxLen >= len(wantStatDataAgg) {
				t.Errorf("\n+ agg data[%d] = %+v\n", i, statDataAgg[i])
			}
		}
	} else {
		// print Data stat diff
		for k, sNode := range statDataSum {
			if wNode, ok := wantStatDataSum[k]; ok {
				if !reflect.DeepEqual(*sNode, *wNode) {
					t.Errorf("\n- data[%+v] = %+v\n+ data[%+v] = %+v\n", k, *wNode, k, *sNode)
				}
			} else {
				t.Errorf("\n+ data[%+v] = %+v\n", k, *sNode)
			}
		}
		for k, wNode := range wantStatDataSum {
			if _, ok := statDataSum[k]; !ok {
				t.Errorf("\n- data[%+v] = %+v\n", k, *wNode)
			}
		}
	}
}
