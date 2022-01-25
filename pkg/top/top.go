package top

import (
	"sort"

	"github.com/msaf1980/graphite-clickhouse-stat/pkg/stat"
)

func GetTop(queries map[string]*stat.Stat, n int, sortKey stat.Sort, nonCompleted bool) []*stat.Stat {
	stats := make([]*stat.Stat, 0, len(queries))

	for _, s := range queries {
		if s.RequestStatus > 0 || nonCompleted {
			stats = append(stats, s)
		}
	}

	sort.SliceStable(stats, func(i, j int) bool {
		return stat.LessStat(stats[i], stats[j], sortKey)
	})

	for id, s := range queries {
		if s.RequestStatus > 0 {
			delete(queries, id)
		}
	}

	if n < len(stats) {
		return stats[len(stats)-n:]
	}

	return stats
}
