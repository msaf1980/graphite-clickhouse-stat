package top

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
)

func Test_GetTop(t *testing.T) {
	tests := []struct {
		name    string
		queries map[string]*stat.Stat
		n       int
		cleanup bool
		key     stat.Sort
		from    int64
		until   int64
		want    []*stat.Stat
	}{
		{
			name: "Top 4 by time",
			queries: map[string]*stat.Stat{
				"1":  {Id: "1", RequestStatus: 200, RequestTime: 1, QueryTime: 1, IndexReadRows: 2, DataReadRows: 3, ReadRows: 5},
				"_2": {Id: "_2", RequestStatus: 200, RequestTime: 2, QueryTime: 2, IndexReadRows: 1, DataReadRows: 2, ReadRows: 3},
				"3":  {Id: "3", RequestStatus: 200, RequestTime: 2, QueryTime: 2, IndexReadRows: 2, DataReadRows: 2, ReadRows: 4},
				"4":  {Id: "4", RequestStatus: 200, RequestTime: 3, QueryTime: 3, IndexReadRows: 2, DataReadRows: 2, ReadRows: 4},
				"5":  {Id: "5", RequestStatus: 504, RequestTime: 30, QueryTime: 30},
				// non-complete request
				"6": {Id: "6", IndexReadRows: 2, ReadRows: 2},
			},
			n:   4,
			key: stat.SortQTime,
			want: []*stat.Stat{
				{Id: "_2", RequestStatus: 200, RequestTime: 2, QueryTime: 2, IndexReadRows: 1, DataReadRows: 2, ReadRows: 3},
				{Id: "3", RequestStatus: 200, RequestTime: 2, QueryTime: 2, IndexReadRows: 2, DataReadRows: 2, ReadRows: 4},
				{Id: "4", RequestStatus: 200, RequestTime: 3, QueryTime: 3, IndexReadRows: 2, DataReadRows: 2, ReadRows: 4},
				{Id: "5", RequestStatus: 504, RequestTime: 30, QueryTime: 30},
			},
		},

		{
			name: "Top 4 by read_rows",
			queries: map[string]*stat.Stat{
				"1":  {Id: "1", RequestStatus: 200, RequestTime: 1, QueryTime: 1, IndexReadRows: 2, DataReadRows: 3},
				"_2": {Id: "_2", RequestStatus: 200, RequestTime: 2, QueryTime: 2, IndexReadRows: 1, DataReadRows: 2},
				"3":  {Id: "3", RequestStatus: 200, RequestTime: 2, QueryTime: 2, IndexReadRows: 2, DataReadRows: 2},
				"4":  {Id: "4", RequestStatus: 200, RequestTime: 3, QueryTime: 3, IndexReadRows: 2, DataReadRows: 2},
				"5":  {Id: "5", RequestStatus: 504, RequestTime: 30, QueryTime: 30},
				// non-complete request
				"6": {Id: "6", IndexReadRows: 2},
			},
			n:   6,
			key: stat.SortReadRows,
			want: []*stat.Stat{
				{Id: "_2", RequestStatus: 200, RequestTime: 2, QueryTime: 2, IndexReadRows: 1, DataReadRows: 2, ReadRows: 3},
				{Id: "3", RequestStatus: 200, RequestTime: 2, QueryTime: 2, IndexReadRows: 2, DataReadRows: 2, ReadRows: 4},
				{Id: "4", RequestStatus: 200, RequestTime: 3, QueryTime: 3, IndexReadRows: 2, DataReadRows: 2, ReadRows: 4},
				{Id: "1", RequestStatus: 200, RequestTime: 1, QueryTime: 1, IndexReadRows: 2, DataReadRows: 3, ReadRows: 5},
				{Id: "5", RequestStatus: 504, RequestTime: 30, QueryTime: 30},
			},
		},

		{
			name: "Top 4 by index_read_rows",
			queries: map[string]*stat.Stat{
				"1":  {Id: "1", RequestStatus: 200, RequestTime: 1, IndexReadRows: 3, DataReadRows: 1},
				"_2": {Id: "_2", RequestStatus: 200, RequestTime: 0.01, IndexReadRows: 1, DataReadRows: 1},
				"3":  {Id: "3", RequestStatus: 200, Points: 1024, RequestTime: 0.03, IndexReadRows: 2, DataReadRows: 1},
				"4":  {Id: "4", RequestStatus: 504, RequestTime: 30, QueryTime: 30},
				"5":  {Id: "5", RequestStatus: 200, Points: 1, RequestTime: 0.04, IndexReadRows: 2, DataReadRows: 1},
				// non-complete request
				"6": {Id: "6", IndexReadRows: 2},
			},
			n:   4,
			key: stat.SortIndexReadRows,
			want: []*stat.Stat{
				{Id: "5", RequestStatus: 200, Points: 1, RequestTime: 0.04, IndexReadRows: 2, DataReadRows: 1, ReadRows: 3},
				{Id: "3", RequestStatus: 200, Points: 1024, RequestTime: 0.03, IndexReadRows: 2, DataReadRows: 1, ReadRows: 3},
				{Id: "1", RequestStatus: 200, RequestTime: 1, IndexReadRows: 3, DataReadRows: 1, ReadRows: 4},
				{Id: "4", RequestStatus: 504, RequestTime: 30, QueryTime: 30},
			},
		},

		{
			name: "Top 4 by data_read_rows",
			queries: map[string]*stat.Stat{
				"1":  {Id: "1", RequestStatus: 200, Metrics: 2, Points: 1, RequestTime: 1, QueryTime: 1, IndexReadRows: 3, DataReadRows: 1},
				"_2": {Id: "_2", RequestStatus: 200, Metrics: 1, Points: 1, RequestTime: 0.01, QueryTime: 0.01, IndexReadRows: 1, DataReadRows: 1},
				"3":  {Id: "3", RequestStatus: 200, Points: 1024, RequestTime: 0.03, QueryTime: 0.03, IndexReadRows: 2, DataReadRows: 2},
				"4":  {Id: "4", RequestStatus: 504, RequestTime: 30, QueryTime: 30},
				"5":  {Id: "5", RequestStatus: 200, Metrics: 1, Points: 1, RequestTime: 0.04, QueryTime: 0.04, IndexReadRows: 2, DataReadRows: 1},
				// non-complete request
				"6": {Id: "6", IndexReadRows: 2},
			},
			n:   4,
			key: stat.SortDataReadRows,
			want: []*stat.Stat{
				{Id: "5", RequestStatus: 200, Metrics: 1, Points: 1, RequestTime: 0.04, QueryTime: 0.04, IndexReadRows: 2, DataReadRows: 1, ReadRows: 3},
				{Id: "1", RequestStatus: 200, Metrics: 2, Points: 1, RequestTime: 1, QueryTime: 1, IndexReadRows: 3, DataReadRows: 1, ReadRows: 4},
				{Id: "3", RequestStatus: 200, Points: 1024, RequestTime: 0.03, QueryTime: 0.03, IndexReadRows: 2, DataReadRows: 2, ReadRows: 4},
				{Id: "4", RequestStatus: 504, RequestTime: 30, QueryTime: 30},
			},
		},

		{
			name: "Top from exclude",
			queries: map[string]*stat.Stat{
				"1": {Id: "1", RequestStatus: 504, RequestTime: 30, QueryTime: 30, TimeStamp: 1674886960 * 1e9},
				"2": {Id: "2", RequestStatus: 504, RequestTime: 30, QueryTime: 30, TimeStamp: 1674886980 * 1e9},
			},
			n:     4,
			key:   stat.SortDataReadRows,
			from:  1674886980 * 1e9,
			until: 1674887060 * 1e9,
			want: []*stat.Stat{
				{Id: "2", RequestStatus: 504, RequestTime: 30, QueryTime: 30, TimeStamp: 1674886980 * 1e9},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, s := range tt.queries {
				s.ReadRows = s.IndexReadRows + s.DataReadRows
				s.ReadBytes = s.IndexReadBytes + s.DataReadBytes
			}
			if got := GetTop(tt.queries, tt.n, tt.key, tt.from, tt.until, tt.cleanup); !reflect.DeepEqual(got, tt.want) {
				maxLen := len(got)
				if maxLen < len(tt.want) {
					maxLen = len(tt.want)
				}
				if len(got) != len(tt.want) {
					t.Errorf("GetTop() len = %d, want %d", len(got), len(tt.want))
				}
				for i := 0; i < maxLen; i++ {
					if i < len(got) && i < len(tt.want) {
						if !reflect.DeepEqual(got[i], tt.want[i]) {
							t.Errorf("\n  [%d] = %s", i, cmp.Diff(tt.want[i], got[i]))
						}
					} else if i >= len(got) {
						t.Errorf("\n- [%d] = %+v\n", i, *tt.want[i])
					} else if maxLen >= len(tt.want) {
						t.Errorf("\n+ [%d] = %+v\n", i, *got[i])
					}
				}
			}

		})
	}
}
