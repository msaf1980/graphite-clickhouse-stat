package aggregate

import (
	"fmt"
)

type AggSortKey int8

const (
	AggSortMax AggSortKey = iota
	AggSortP99
	AggSortP95
	AggSortP90
	AggSortP50
)

var aggSortKeyStrings []string = []string{"max", "p99", "p95", "p90", "p50"}

func SortKeyStrings() []string {
	return aggSortKeyStrings
}

func (s *AggSortKey) Set(value string, _ bool) error {
	switch value {
	case "p99":
		*s = AggSortP99
	case "p95":
		*s = AggSortP95
	case "p90":
		*s = AggSortP90
	case "p50":
		*s = AggSortP50
	case "max":
		*s = AggSortMax
	default:
		return fmt.Errorf("invalid agg sort key %s", value)
	}
	return nil
}

func (s *AggSortKey) String() string {
	return aggSortKeyStrings[*s]
}

func (s *AggSortKey) Type() string {
	return "agg_sort_key"
}

func (s *AggSortKey) Reset(i interface{}) {
	*s = i.(AggSortKey)
}

func (s *AggSortKey) Get() interface{} {
	return s.GetSortKey()
}

func (s *AggSortKey) GetSortKey() AggSortKey {
	return *s
}
