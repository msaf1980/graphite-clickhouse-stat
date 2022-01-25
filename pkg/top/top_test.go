package top

import (
	"reflect"
	"testing"

	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
)

func Test_GetTop(t *testing.T) {
	tests := []struct {
		name         string
		queries      map[string]*stat.Stat
		n            int
		nonCompleted bool
		key          stat.Sort
		want         []*stat.Stat
	}{
		{
			name: "Top 4 by time",
			queries: map[string]*stat.Stat{
				"1": {
					Id:            "1",
					RequestStatus: 200,
					RequestTime:   1,
					IndexReadRows: 2,
					DataReadRows:  3,
				},
				"_2": {
					Id:            "_2",
					RequestStatus: 200,
					RequestTime:   2,
					IndexReadRows: 1,
					DataReadRows:  2,
				},
				"3": {
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   2,
					IndexReadRows: 2,
					DataReadRows:  2,
				},
				"4": {
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   3,
					IndexReadRows: 2,
					DataReadRows:  2,
				},
				"5": {
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					IndexReadRows: 2,
				},
				"6": { // non-complete request
					Id:            "6",
					IndexReadRows: 2,
					IndexTime:     29,
				},
			},
			n:   4,
			key: stat.SortTime,
			want: []*stat.Stat{
				{
					Id:            "_2",
					RequestStatus: 200,
					RequestTime:   2,
					IndexReadRows: 1,
					DataReadRows:  2,
				},
				{
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   2,
					IndexReadRows: 2,
					DataReadRows:  2,
				},
				{
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   3,
					IndexReadRows: 2,
					DataReadRows:  2,
				},
				{
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					IndexReadRows: 2,
				},
			},
		},

		{
			name: "Top 4 by time (non-completed)",
			queries: map[string]*stat.Stat{
				"1": {
					Id:            "1",
					RequestStatus: 200,
					RequestTime:   1,
					IndexReadRows: 2,
					DataReadRows:  3,
				},
				"_2": {
					Id:            "_2",
					RequestStatus: 200,
					RequestTime:   2,
					IndexReadRows: 1,
					DataReadRows:  2,
				},
				"3": {
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   2,
					IndexReadRows: 2,
					DataReadRows:  2,
				},
				"4": {
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   3,
					IndexReadRows: 2,
					DataReadRows:  2,
				},
				"5": {
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					IndexReadRows: 2,
				},
				"6": { // non-complete request
					Id:            "6",
					IndexReadRows: 2,
					IndexTime:     29,
				},
			},
			n:            4,
			nonCompleted: true,
			key:          stat.SortTime,
			want: []*stat.Stat{
				{
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   2,
					IndexReadRows: 2,
					DataReadRows:  2,
				},
				{
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   3,
					IndexReadRows: 2,
					DataReadRows:  2,
				},
				{ // non-complete request
					Id:            "6",
					IndexReadRows: 2,
					IndexTime:     29,
				},
				{
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					IndexReadRows: 2,
				},
			},
		},

		{
			name: "Top 4 by read_rows",
			queries: map[string]*stat.Stat{
				"1": {
					Id:            "1",
					RequestStatus: 200,
					RequestTime:   1,
					IndexReadRows: 2,
					DataReadRows:  3,
				},
				"_2": {
					Id:            "_2",
					RequestStatus: 200,
					RequestTime:   2,
					IndexReadRows: 1,
					DataReadRows:  2,
				},
				"3": {
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   2,
					IndexReadRows: 2,
					DataReadRows:  2,
				},
				"4": {
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   3,
					IndexReadRows: 2,
					DataReadRows:  2,
				},
				"5": {
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					IndexReadRows: 2,
					DataReadRows:  0,
				},
				"6": { // non-complete request
					Id:            "6",
					IndexReadRows: 2,
					IndexTime:     29,
				},
			},
			n:   4,
			key: stat.SortReadRows,
			want: []*stat.Stat{
				{
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   2,
					IndexReadRows: 2,
					DataReadRows:  2,
				},
				{
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   3,
					IndexReadRows: 2,
					DataReadRows:  2,
				},
				{
					Id:            "1",
					RequestStatus: 200,
					RequestTime:   1,
					IndexReadRows: 2,
					DataReadRows:  3,
				},
				{
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					IndexReadRows: 2,
				},
			},
		},

		{
			name: "Top 4 by index_read_rows",
			queries: map[string]*stat.Stat{
				"1": {
					Id:            "1",
					RequestStatus: 200,
					RequestTime:   1,
					IndexReadRows: 3,
					IndexTime:     1,
				},
				"_2": {
					Id:            "_2",
					RequestStatus: 200,
					RequestTime:   0.01,
					IndexReadRows: 1,
					IndexTime:     0.01,
				},
				"3": {
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   0.04,
					IndexReadRows: 2,
					IndexTime:     0.04,
				},
				"4": {
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   0.03,
					IndexReadRows: 2,
					IndexTime:     0.03,
				},
				"5": {
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					IndexReadRows: 2,
					IndexTime:     30,
				},
				"6": { // non-complete request
					Id:            "6",
					IndexReadRows: 2,
					IndexTime:     29,
				},
			},
			n:   4,
			key: stat.SortIndexReadRows,
			want: []*stat.Stat{
				{
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   0.03,
					IndexReadRows: 2,
					IndexTime:     0.03,
				},
				{
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   0.04,
					IndexReadRows: 2,
					IndexTime:     0.04,
				},
				{
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					IndexReadRows: 2,
					IndexTime:     30,
				},
				{
					Id:            "1",
					RequestStatus: 200,
					RequestTime:   1,
					IndexReadRows: 3,
					IndexTime:     1,
				},
			},
		},

		{
			name: "Top 4 by index_time",
			queries: map[string]*stat.Stat{
				"1": {
					Id:            "1",
					RequestStatus: 200,
					RequestTime:   1,
					IndexReadRows: 3,
					IndexTime:     1,
				},
				"_2": {
					Id:            "_2",
					RequestStatus: 200,
					RequestTime:   2,
					IndexReadRows: 1,
					IndexTime:     0.01,
				},
				"3": {
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   0.04,
					IndexReadRows: 2,
					IndexTime:     0.04,
				},
				"4": {
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   0.03,
					IndexReadRows: 2,
					IndexTime:     0.03,
				},
				"5": {
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					IndexReadRows: 2,
					IndexTime:     30,
				},
				"6": { // non-complete request
					Id:            "6",
					IndexReadRows: 2,
					IndexTime:     29,
				},
			},
			n:   4,
			key: stat.SortIndexTime,
			want: []*stat.Stat{
				{
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   0.03,
					IndexReadRows: 2,
					IndexTime:     0.03,
				},
				{
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   0.04,
					IndexReadRows: 2,
					IndexTime:     0.04,
				},
				{
					Id:            "1",
					RequestStatus: 200,
					RequestTime:   1,
					IndexReadRows: 3,
					IndexTime:     1,
				},
				{
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					IndexReadRows: 2,
					IndexTime:     30,
				},
			},
		},

		{
			name: "Top 4 by data_read_rows",
			queries: map[string]*stat.Stat{
				"1": {
					Id:            "1",
					RequestStatus: 200,
					RequestTime:   1,
					DataReadRows:  3,
					DataTime:      1,
				},
				"_2": {
					Id:            "_2",
					RequestStatus: 200,
					RequestTime:   0.01,
					DataReadRows:  1,
					DataTime:      0.01,
				},
				"3": {
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   0.04,
					DataReadRows:  2,
					DataTime:      0.04,
				},
				"4": {
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   0.03,
					DataReadRows:  2,
					DataTime:      0.03,
				},
				"5": {
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					DataReadRows:  2,
					DataTime:      30,
				},
				"6": { // non-complete request
					Id:            "6",
					IndexReadRows: 2,
					IndexTime:     29,
				},
			},
			n:   4,
			key: stat.SortDataReadRows,
			want: []*stat.Stat{
				{
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   0.03,
					DataReadRows:  2,
					DataTime:      0.03,
				},
				{
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   0.04,
					DataReadRows:  2,
					DataTime:      0.04,
				},
				{
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					DataReadRows:  2,
					DataTime:      30,
				},
				{
					Id:            "1",
					RequestStatus: 200,
					RequestTime:   1,
					DataReadRows:  3,
					DataTime:      1,
				},
			},
		},

		{
			name: "Top 3 by data_time",
			queries: map[string]*stat.Stat{
				"1": {
					Id:            "1",
					RequestStatus: 200,
					RequestTime:   1,
					DataReadRows:  3,
					DataTime:      1,
				},
				"_2": {
					Id:            "_2",
					RequestStatus: 200,
					RequestTime:   0.01,
					DataReadRows:  1,
					DataTime:      0.01,
				},
				"3": {
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   0.04,
					DataReadRows:  2,
					DataTime:      0.04,
				},
				"4": {
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   0.03,
					DataReadRows:  2,
					DataTime:      0.03,
				},
				"5": {
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					DataReadRows:  2,
					DataTime:      30,
				},
				"6": { // non-complete request
					Id:            "6",
					IndexReadRows: 2,
					IndexTime:     29,
				},
			},
			n:   4,
			key: stat.SortDataTime,
			want: []*stat.Stat{
				{
					Id:            "4",
					RequestStatus: 200,
					RequestTime:   0.03,
					DataReadRows:  2,
					DataTime:      0.03,
				},
				{
					Id:            "3",
					RequestStatus: 200,
					RequestTime:   0.04,
					DataReadRows:  2,
					DataTime:      0.04,
				},
				{
					Id:            "1",
					RequestStatus: 200,
					RequestTime:   1,
					DataReadRows:  3,
					DataTime:      1,
				},
				{
					Id:            "5",
					RequestStatus: 503,
					RequestTime:   30,
					DataReadRows:  2,
					DataTime:      30,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetTop(tt.queries, tt.n, tt.key, tt.nonCompleted); !reflect.DeepEqual(got, tt.want) {
				maxLen := len(got)
				if maxLen < len(tt.want) {
					maxLen = len(tt.want)
				}
				for i := 0; i < maxLen; i++ {
					if i < len(got) && i < len(tt.want) {
						if *got[i] != *tt.want[i] {
							t.Errorf("\n- [%d] = %+v\n+ [%d] = %+v\n", i, *tt.want[i], i, *got[i])
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
