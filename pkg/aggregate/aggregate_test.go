package aggregate

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
)

func Test_StatSummary_append(t *testing.T) {
	stats := []*stat.Stat{
		{
			RequestType: "render", Id: "1f72e822bed05bebd97a9bdcc4654f1a",
			TimeStamp:     1674288343773000000,
			Metrics:       1,
			Points:        4,
			Bytes:         148,
			RequestStatus: 200, RequestTime: 3, QueryTime: 3,
			WaitStatus: stat.StatusSuccess,
			ReadRows:   414 + 12284, ReadBytes: 14168 + 2497094,
			Queries:       []stat.Query{{Query: "test.a", Days: 1, From: 1674288343 - 60, Until: 1674288343}},
			IndexReadRows: 414, IndexReadBytes: 14168,
			Index: []stat.IndexStat{
				{
					Status: stat.StatusSuccess, Time: 1,
					ReadRows: 2414, ReadBytes: 1416887,
					Table:   "graphite_indexd",
					QueryId: "1f72e822bed05bebd97a9bdcc4654f1a::1390f060ca3d959d",
					Days:    1,
				},
			},
			DataReadRows: 12284, DataReadBytes: 16497094,
			Data: []stat.DataStat{
				{
					Status: stat.StatusSuccess, Time: 2,
					ReadRows: 12284, ReadBytes: 2497094,
					Table:   "graphite_reversed",
					QueryId: "1f72e822bed05bebd97a9bdcc4654f1a::1b87069be1c53ee2",
					Days:    1, From: 1674288230, Until: 1674288349,
				},
			},
		},
		{
			RequestType: "render", Id: "1f72e822bed05bebd97a9bdcc4654f1b",
			TimeStamp:     1674288343773000000,
			Metrics:       1,
			Points:        4,
			Bytes:         148,
			RequestStatus: 200, RequestTime: 2, QueryTime: 2,
			WaitStatus: stat.StatusSuccess,
			ReadRows:   12284, ReadBytes: 2497094,
			Queries: []stat.Query{{Query: "test.a", Days: 1, From: 1674288403 - 3600*25, Until: 1674288403 - 3600*24}},
			Index: []stat.IndexStat{
				{Status: stat.StatusCached, Days: 1},
			},
			DataReadRows: 12284, DataReadBytes: 16497094,
			Data: []stat.DataStat{
				{
					Status: stat.StatusSuccess, Time: 2,
					ReadRows: 12284, ReadBytes: 2497094,
					Table:   "graphite_reversed",
					QueryId: "1f72e822bed05bebd97a9bdcc4654f1b::1b87069be1c53ee2",
					Days:    1, From: 1674288403 - 3600*25, Until: 1674288403 - 3600*24,
				},
			},
		},
		{
			RequestType: "render", Id: "1f72e822bed05bebd97a9bdcc4654f1c",
			TimeStamp:     1674288343773000000,
			Metrics:       1,
			RequestStatus: 504, RequestTime: 10, QueryTime: 10,
			WaitStatus: stat.StatusSuccess,
			Queries:    []stat.Query{{Query: "test.a", Days: 1, From: 1674288403 - 3600*25, Until: 1674288403 - 3600*24}},
			Index: []stat.IndexStat{
				{Status: stat.StatusCached, Days: 1},
			},
			Data: []stat.DataStat{
				{
					Status: stat.StatusError, Time: 10,
					Days: 1, From: 1674288403 - 3600*25, Until: 1674288403 - 3600*24,
				},
			},
		},
		{
			RequestType: "render", Id: "1f72e822bed05bebd97a9bdcc4654f1d",
			TimeStamp:     1674288343773000000,
			Metrics:       1,
			RequestStatus: 504, RequestTime: 10, QueryTime: 10,
			WaitStatus: stat.StatusSuccess,
			Queries:    []stat.Query{{Query: "test.a", Days: 1, From: 1674288403 - 3600*25, Until: 1674288403 - 3600*24}},
			Index: []stat.IndexStat{
				{Status: stat.StatusError, Time: 10, Days: 1},
			},
			Data: []stat.DataStat{},
		},
	}

	wantAggStatSum := &StatAggSum{
		Index: map[LabelKey][]*StatIndexAggNode{
			{DurationLabel: "1d", RequestType: "render"}: {
				{
					IndexKey: StatKey{
						Queries: "[{query='test.a',index=1d}]", DurationLabel: "1d", RequestType: "render",
					},
					Queries:  []StatQuery{{Query: "test.a", DurationLabel: "1d"}},
					SampleId: "1f72e822bed05bebd97a9bdcc4654f1a", ErrorId: "1f72e822bed05bebd97a9bdcc4654f1d",
					N: 4, ErrorsPcnt: 25, IndexCacheHitPcnt: 66.66666666666666,
					// IndexN:    AggNode{Min: 1, Max: 1, P50: 1, P90: 1, P95: 1, P99: 1},
					Metrics:   AggNode{Min: 1, Max: 1, P50: 1, P90: 1, P95: 1, P99: 1, Init: true},
					ReadRows:  AggNode{Min: 0, Max: 414, P50: 0, P90: 207, P95: 207, P99: 207, Init: true},
					ReadBytes: AggNode{Min: 0, Max: 14168, P50: 0, P90: 7084, P95: 7084, P99: 7084, Init: true},
					Times:     AggNode{Min: 0, Max: 10, P50: 0, P90: 5.5, P95: 5.5, P99: 5.5, Init: true},
				},
			},
		},
		Requests: map[LabelKey][]*StatRequestAggNode{
			{DurationLabel: "10m", RequestType: "render"}: {
				{
					IndexKey: StatKey{Queries: "[{query='test.a',index=1d}]", DurationLabel: "1d", RequestType: "render"},
					DataKey:  StatKey{Queries: "[{query='test.a',render=10m}]", DurationLabel: "10m", RequestType: "render"},
					Queries:  []StatQuery{{Query: "test.a", DurationLabel: "10m"}},
					SampleId: "1f72e822bed05bebd97a9bdcc4654f1a",
					N:        1, RequestStatus: map[int64]int64{200: 1},
					Metrics:       AggNode{Min: 1, Max: 1, P50: 1, P90: 1, P95: 1, P99: 1, Init: true},
					Points:        AggNode{Min: 4, Max: 4, P50: 4, P90: 4, P95: 4, P99: 4, Init: true},
					ReadRows:      AggNode{Min: 12698, Max: 12698, P50: 12698, P90: 12698, P95: 12698, P99: 12698, Init: true},
					ReadBytes:     AggNode{Min: 2511262, Max: 2511262, P50: 2511262, P90: 2511262, P95: 2511262, P99: 2511262, Init: true},
					DataReadRows:  AggNode{Min: 12284, Max: 12284, P50: 12284, P90: 12284, P95: 12284, P99: 12284, Init: true},
					DataReadBytes: AggNode{Min: 16497094, Max: 16497094, P50: 16497094, P90: 16497094, P95: 16497094, P99: 16497094, Init: true},
					RequestTimes:  AggNode{Min: 3, Max: 3, P50: 3, P90: 3, P95: 3, P99: 3, Init: true},
					QueryTimes:    AggNode{Min: 3, Max: 3, P50: 3, P90: 3, P95: 3, P99: 3, Init: true},
					DataTimes:     AggNode{Min: 2, Max: 2, P50: 2, P90: 2, P95: 2, P99: 2, Init: true},
				},
			},
			{DurationLabel: "1h", RequestType: "render"}: {
				{
					IndexKey: StatKey{Queries: "[{query='test.a',index=1d}]", DurationLabel: "1d", RequestType: "render"},
					DataKey:  StatKey{Queries: "[{query='test.a',render=1h}]", DurationLabel: "1h", RequestType: "render"},
					Queries:  []StatQuery{{Query: "test.a", DurationLabel: "1h"}},
					SampleId: "1f72e822bed05bebd97a9bdcc4654f1b",
					// SampleQueryIds: []string{"1f72e822bed05bebd97a9bdcc4654f1b::1b87069be1c53ee2"},
					ErrorId: "1f72e822bed05bebd97a9bdcc4654f1c",
					N:       3, ErrorsPcnt: 66.66666666666666, RequestStatus: map[int64]int64{200: 1, 504: 2},
					DataErrorsPcnt: 33.33333333333333, IndexErrorsPcnt: 33.33333333333333, IndexCacheHitPcnt: 100,
					Metrics:       AggNode{Min: 1, Max: 1, P50: 1, P90: 1, P95: 1, P99: 1, Init: true},
					Points:        AggNode{Min: 4, Max: 4, P50: 4, P90: 4, P95: 4, P99: 4, Init: true},
					ReadRows:      AggNode{Min: 12284, Max: 12284, P50: 12284, P90: 12284, P95: 12284, P99: 12284, Init: true},
					ReadBytes:     AggNode{Min: 2497094, Max: 2497094, P50: 2497094, P90: 2497094, P95: 2497094, P99: 2497094, Init: true},
					RequestTimes:  AggNode{Min: 2, Max: 10, P50: 6, P90: 10, P95: 10, P99: 10, Init: true},
					QueryTimes:    AggNode{Min: 2, Max: 10, P50: 6, P90: 10, P95: 10, P99: 10, Init: true},
					DataReadRows:  AggNode{Min: 12284, Max: 12284, P50: 12284, P90: 12284, P95: 12284, P99: 12284, Init: true},
					DataReadBytes: AggNode{Min: 16497094, Max: 16497094, P50: 16497094, P90: 16497094, P95: 16497094, P99: 16497094, Init: true},
					DataTimes:     AggNode{Min: 2, Max: 10, P50: 2, P90: 6, P95: 6, P99: 6, Init: true},
				},
			},
		},
	}

	statSum := NewStatSummary()

	for _, s := range stats {
		statSum.Append(s)
	}
	aggSum := statSum.Aggregate()

	if !reflect.DeepEqual(aggSum, wantAggStatSum) {
		t.Errorf("StatSummary.Append(...) = %s", cmp.Diff(wantAggStatSum, aggSum))
	}

	// 	// print Index agg stat diff
	// 	maxLen := len(statIndexAgg)
	// 	if maxLen < len(wantStatIndexAgg) {
	// 		maxLen = len(wantStatIndexAgg)
	// 	}
	// 	for i := 0; i < maxLen; i++ {
	// 		if i < len(statIndexAgg) && i < len(wantStatIndexAgg) {
	// 			if statIndexAgg[i] != wantStatIndexAgg[i] {
	// 				t.Errorf("\n- agg index[%d] = %+v\n+ agg index[%d] = %+v\n", i, wantStatIndexAgg[i], i, statIndexAgg[i])
	// 			}
	// 		} else if i >= len(statIndexAgg) {
	// 			t.Errorf("\n- agg index[%d] = %+v\n", i, wantStatIndexAgg[i])
	// 		} else if maxLen >= len(wantStatIndexAgg) {
	// 			t.Errorf("\n+ agg index[%d] = %+v\n", i, statIndexAgg[i])
	// 		}
	// 	}
	// } else {
	// 	// print Index stat diff
	// 	for k, sNode := range statIndexSum {
	// 		if wNode, ok := wantStatIndexSum[k]; ok {
	// 			if !reflect.DeepEqual(*sNode, *wNode) {
	// 				t.Errorf("\n- index[%+v] = %+v\n+ index[%+v] = %+v\n", k, *wNode, k, *sNode)
	// 			}
	// 		} else {
	// 			t.Errorf("\n+ index[%+v] = %+v\n", k, *sNode)
	// 		}
	// 	}
	// 	for k, wNode := range wantStatIndexSum {
	// 		if _, ok := statIndexSum[k]; !ok {
	// 			t.Errorf("\n- index[%+v] = %+v\n", k, *wNode)
	// 		}
	// 	}
	// }

	// if reflect.DeepEqual(statDataSum, wantStatDataSum) {
	// 	statDataAgg := statDataSum.Aggregate()

	// 	sort.SliceStable(statDataAgg, func(i, j int) bool {
	// 		if statDataAgg[i].Key.Target == statDataAgg[j].Key.Target {
	// 			return statDataAgg[i].Key.Duration < statDataAgg[j].Key.Duration
	// 		}
	// 		return statDataAgg[i].Key.Target < statDataAgg[j].Key.Target
	// 	})

	// 	// print Data agg stat diff
	// 	maxLen := len(statDataAgg)
	// 	if maxLen < len(wantStatDataAgg) {
	// 		maxLen = len(wantStatDataAgg)
	// 	}
	// 	for i := 0; i < maxLen; i++ {
	// 		if i < len(statDataAgg) && i < len(wantStatDataAgg) {
	// 			if statDataAgg[i] != wantStatDataAgg[i] {
	// 				t.Errorf("\n- agg data[%d] = %+v\n+ agg data[%d] = %+v\n", i, wantStatDataAgg[i], i, statDataAgg[i])
	// 			}
	// 		} else if i >= len(statDataAgg) {
	// 			t.Errorf("\n- agg data[%d] = %+v\n", i, wantStatDataAgg[i])
	// 		} else if maxLen >= len(wantStatDataAgg) {
	// 			t.Errorf("\n+ agg data[%d] = %+v\n", i, statDataAgg[i])
	// 		}
	// 	}
	// } else {
	// 	// print Data stat diff
	// 	for k, sNode := range statDataSum {
	// 		if wNode, ok := wantStatDataSum[k]; ok {
	// 			if !reflect.DeepEqual(*sNode, *wNode) {
	// 				t.Errorf("\n- data[%+v] = %+v\n+ data[%+v] = %+v\n", k, *wNode, k, *sNode)
	// 			}
	// 		} else {
	// 			t.Errorf("\n+ data[%+v] = %+v\n", k, *sNode)
	// 		}
	// 	}
	// 	for k, wNode := range wantStatDataSum {
	// 		if _, ok := statDataSum[k]; !ok {
	// 			t.Errorf("\n- data[%+v] = %+v\n", k, *wNode)
	// 		}
	// 	}
	// }
}
