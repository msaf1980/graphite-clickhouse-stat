package aggregate

import "fmt"

type RequestSort int8

const (
	RequestSortQTime RequestSort = iota
	RequestSortRTime
	RequestSortDTime
	RequestSortReadRows
	RequestSortIndexReadRows
	RequestSortDataReadRows
	RequestSortQueries
	RequestSortErrors
)

func (s *RequestSort) Reset(i interface{}) {
	*s = i.(RequestSort)
}

func (s *RequestSort) Get() interface{} {
	return s.GetSort()
}

func (s *RequestSort) GetSort() RequestSort {
	return *s
}

var requestSortStrings []string = []string{"qtime", "rtime", "data_time", "read_rows", "index_read_rows", "data_read_rows", "queries", "errors"}

func RequestSortStrings() []string {
	return requestSortStrings
}

func (s *RequestSort) Set(value string, _ bool) error {
	switch value {
	case "read_rows":
		*s = RequestSortReadRows
	case "qtime":
		*s = RequestSortQTime
	case "rtime":
		*s = RequestSortRTime
	case "data_time":
		*s = RequestSortDTime
	case "index_read_rows":
		*s = RequestSortIndexReadRows
	case "data_read_rows":
		*s = RequestSortDataReadRows
	case "queries":
		*s = RequestSortQueries
	case "errors":
		*s = RequestSortErrors
	default:
		return fmt.Errorf("invalid sort %s", value)
	}
	return nil
}

func (s *RequestSort) String() string {
	return requestSortStrings[*s]
}

func (s *RequestSort) Type() string {
	return "agg_req_sort"
}
