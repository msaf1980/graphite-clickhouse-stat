package aggregate

import "fmt"

type IndexSort int8

const (
	IndexSortTime IndexSort = iota
	IndexSortReadRows
	IndexSortQueries
	IndexSortErrors
)

func (s *IndexSort) Reset(i interface{}) {
	*s = i.(IndexSort)
}

func (s *IndexSort) Get() interface{} {
	return s.GetSort()
}

func (s *IndexSort) GetSort() IndexSort {
	return *s
}

var indexSortStrings []string = []string{"time", "read_rows", "queries", "errors"}

func IndexSortStrings() []string {
	return indexSortStrings
}

func (s *IndexSort) Set(value string, _ bool) error {
	switch value {
	case "read_rows":
		*s = IndexSortReadRows
	case "time":
		*s = IndexSortTime
	case "queries":
		*s = IndexSortQueries
	case "errors":
		*s = IndexSortErrors
	default:
		return fmt.Errorf("invalid sort %s", value)
	}
	return nil
}

func (s *IndexSort) String() string {
	return indexSortStrings[*s]
}

func (s *IndexSort) Type() string {
	return "agg_index_sort"
}
