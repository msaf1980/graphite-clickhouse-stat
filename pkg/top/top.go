package top

import (
	"sort"

	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
)

func GetTop(queries map[string]*stat.Stat, n int, sortKey stat.Sort, from, until int64, cleanup bool) []*stat.Stat {
	stats := make([]*stat.Stat, 0, len(queries))

	for id, s := range queries {
		// check for completed record
		if s.RequestStatus > 0 {
			add := true
			if from > 0 && s.TimeStamp < from {
				add = false
			}
			if add && until > 0 && s.TimeStamp >= until {
				add = false
			}
			if add {
				stats = append(stats, s)
			}
			if cleanup {
				delete(queries, id)
			}
		}
	}

	sort.SliceStable(stats, func(i, j int) bool {
		return stat.LessStat(stats[i], stats[j], sortKey)
	})

	if n < len(stats) {
		return stats[len(stats)-n:]
	}

	return stats
}
