package aggregate

import (
	"fmt"
	"sort"
)

type StatIndexDiffNode struct {
	StatIndexAggNode

	DiffNPcnt      int64
	DiffErrorsPcnt float64

	DiffMetrics AggNode

	DiffIndexCacheHitPcnt float64

	DiffReadRows  AggNode
	DiffReadBytes AggNode
	DiffTimes     AggNode
}

func GreaterIndexDiffP99ByTime(a, b *StatIndexDiffNode) bool {
	if a.Times.P99 == b.Times.P99 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.Times.P99 > b.Times.P99
}

func GreaterIndexDiffP95ByTime(a, b *StatIndexDiffNode) bool {
	if a.Times.P95 == b.Times.P95 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.Times.P95 > b.Times.P95
}

func GreaterIndexDiffP90ByTime(a, b *StatIndexDiffNode) bool {
	if a.Times.P90 == b.Times.P90 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.Times.P90 > b.Times.P90
}

func GreaterIndexDiffP50ByTime(a, b *StatIndexDiffNode) bool {
	if a.Times.P50 == b.Times.P50 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.Times.P50 > b.Times.P50
}

func GreaterIndexDiffMaxByTime(a, b *StatIndexDiffNode) bool {
	if a.Times.Max == b.Times.Max {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.Times.Max > b.Times.Max
}

func GreaterIndexDiffP99ByRows(a, b *StatIndexDiffNode) bool {
	if a.ReadRows.P99 == b.ReadRows.P99 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.ReadRows.P99 > b.ReadRows.P99
}

func GreaterIndexDiffP95ByRows(a, b *StatIndexDiffNode) bool {
	if a.ReadRows.P95 == b.ReadRows.P95 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.ReadRows.P95 > b.ReadRows.P95
}

func GreaterIndexDiffP90ByRows(a, b *StatIndexDiffNode) bool {
	if a.ReadRows.P90 == b.ReadRows.P90 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.ReadRows.P90 > b.ReadRows.P90
}

func GreaterIndexDiffP50ByRows(a, b *StatIndexDiffNode) bool {
	if a.ReadRows.P50 == b.ReadRows.P50 {
		return a.ReadRows.Max > b.ReadRows.Max
	}
	return a.ReadRows.P50 > b.ReadRows.P50
}

func GreaterIndexDiffMaxByRows(a, b *StatIndexDiffNode) bool {
	if a.ReadRows.Max == b.ReadRows.Max {
		return a.ReadRows.P95 > b.ReadRows.P95
	}
	return a.ReadRows.Max > b.ReadRows.Max
}

func GreaterIndexDiffByQueries(a, b *StatIndexDiffNode) bool {
	if a.N == b.N {
		return a.Times.Max > b.Times.Max
	}
	return a.N > b.N
}

func GreaterIndexDiffByErrors(a, b *StatIndexDiffNode) bool {
	if a.ErrorsPcnt == b.ErrorsPcnt {
		return a.Times.Max > b.Times.Max
	}
	return a.ErrorsPcnt > b.ErrorsPcnt
}

func SortIndexDiff(statIndexAgg []*StatIndexDiffNode, indexSort IndexSort, indexKey AggSortKey) {
	switch indexSort {
	case IndexSortTime:
		switch indexKey {
		case AggSortP99:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexDiffP99ByTime(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortP95:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexDiffP95ByTime(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortP90:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexDiffP90ByTime(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortP50:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexDiffP50ByTime(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortMax:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexDiffMaxByTime(statIndexAgg[i], statIndexAgg[j])
			})
		default:
			panic(fmt.Errorf("unknown agg sort key: %d", indexKey))
		}
	case IndexSortReadRows:
		switch indexKey {
		case AggSortP99:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexDiffP99ByRows(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortP95:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexDiffP95ByRows(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortP90:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexDiffP90ByRows(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortP50:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexDiffP50ByRows(statIndexAgg[i], statIndexAgg[j])
			})
		case AggSortMax:
			sort.SliceStable(statIndexAgg, func(i, j int) bool {
				return GreaterIndexDiffMaxByRows(statIndexAgg[i], statIndexAgg[j])
			})
		default:
			panic(fmt.Errorf("unknown agg sort key: %d", indexKey))
		}
	case IndexSortQueries:
		sort.SliceStable(statIndexAgg, func(i, j int) bool {
			return GreaterIndexDiffByQueries(statIndexAgg[i], statIndexAgg[j])
		})
	case IndexSortErrors:
		sort.SliceStable(statIndexAgg, func(i, j int) bool {
			return GreaterIndexDiffByErrors(statIndexAgg[i], statIndexAgg[j])
		})
	default:
		panic(fmt.Errorf("unknown agg index sort: %d", indexSort))
	}
}
