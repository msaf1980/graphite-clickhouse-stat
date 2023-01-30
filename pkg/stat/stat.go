package stat

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Status int8

const (
	StatusNone = iota
	StatusSuccess
	StatusCached
	StatusError
)

var statusStrings []string = []string{" ", "S", "C", "E"}

func (s *Status) String() string {
	return statusStrings[*s]
}

func stripError(err string) string {
	if strings.Contains(err, " lookup ") {
		return "address lookup error"
	}
	if strings.HasSuffix(err, ": connection refused") {
		return "connection refused"
	}
	if strings.HasSuffix(err, ": broken pipe") ||
		strings.HasSuffix(err, ": connection reset by peer") ||
		strings.HasSuffix(err, "EOF") {
		return "connection reset"
	}
	if strings.Contains(err, ": context canceled") {
		return "context canceled"
	}
	if len(err) > 20 {
		return err[:20]
	}
	return err
}

func readError(logEntry map[string]interface{}, key string) string {
	if item, ok := logEntry[key]; ok {
		if v, ok := item.(string); ok {
			if start := strings.Index(v, " cannot reach "); start >= 0 {
				err := v[start+1:]
				if end := strings.Index(err, ";"); end > 0 {
					err = err[0:end]
				}

				return err
			}

			return stripError(v)
		}
	}

	return ""
}

func readString(logEntry map[string]interface{}, key string) (string, error) {
	if item, ok := logEntry[key]; ok {
		if v, ok := item.(string); ok {
			return v, nil
		} else {
			return "", errors.New("key " + key + "not a string")
		}
	} else {
		return "", errors.New("key " + key + "not found")
	}
}

// func readStringSlice(logEntry map[string]interface{}, key string) ([]string, error) {
// 	if item, ok := logEntry[key]; ok {
// 		if items, ok := item.([]interface{}); ok {
// 			if len(items) == 0 {
// 				return []string{}, nil
// 			}
// 			if _, ok := items[0].(string); ok {
// 				s := make([]string, 0, len(items))
// 				for _, i := range items {
// 					s = append(s, i.(string))
// 				}
// 				return s, nil
// 			}
// 		}
// 		return []string{}, errors.New("key " + key + "not a []string")
// 	} else {
// 		return []string{}, errors.New("key " + key + "not found")
// 	}
// }

func readInt64(logEntry map[string]interface{}, key string) (int64, error) {
	if item, ok := logEntry[key]; ok {
		if v, ok := item.(string); ok {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				return n, nil
			} else {
				return 0, errors.New("key " + key + "not a number")
			}
		} else {
			if n, ok := item.(float64); ok {
				return int64(n), nil
			} else {
				return 0, errors.New("key " + key + "not a number")
			}
		}
	} else {
		return 0, errors.New("key " + key + "not found")
	}
}

func readFloat64(logEntry map[string]interface{}, key string) (float64, error) {
	if item, ok := logEntry[key]; ok {
		if v, ok := item.(string); ok {
			if n, err := strconv.ParseFloat(v, 64); err == nil {
				return n, nil
			} else {
				return 0, errors.New("key " + key + "not a float number")
			}
		} else {
			if n, ok := item.(float64); ok {
				return n, nil
			} else {
				return 0, errors.New("key " + key + "not a float number")
			}
		}
	} else {
		return 0, errors.New("key " + key + "not found")
	}
}

func readBool(logEntry map[string]interface{}, key string) (bool, error) {
	if item, ok := logEntry[key]; ok {
		if v, ok := item.(bool); ok {
			return v, nil
		} else {
			return false, errors.New("key " + key + "not a bool")
		}
	} else {
		return false, errors.New("key " + key + "not found")
	}
}

type Query struct {
	Days  int
	Query string
	From  int64
	Until int64
}

type IndexStat struct {
	Status Status
	// Rows      int64
	ReadRows  int64
	ReadBytes int64
	Time      float64
	Table     string
	QueryId   string
	Days      int
	Error     string
}

type DataStat struct {
	Status Status
	// Rows      int64
	ReadRows  int64
	ReadBytes int64
	Time      float64
	Table     string
	QueryId   string
	Days      int
	From      int64
	Until     int64
	Error     string
}

type Stat struct {
	Id string

	TimeStamp int64

	Queries []Query

	Metrics int64
	Points  int64
	Bytes   int64

	RequestType   string
	RequestTime   float64
	RequestStatus int64
	WaitTime      float64
	WaitStatus    Status
	QueryTime     float64 // RequestTime - WaitTime

	ReadRows  int64
	ReadBytes int64

	IndexReadRows  int64
	IndexReadBytes int64
	Index          []IndexStat

	DataReadRows  int64
	DataReadBytes int64
	Data          []DataStat
}

func (s *Stat) Reset(ts int64) {
	s.TimeStamp = ts
	s.Queries = make([]Query, 0)

	s.Metrics = 0
	s.Points = 0
	s.Bytes = 0

	s.RequestType = ""
	s.RequestTime = 0
	s.RequestStatus = 0

	s.WaitTime = 0
	s.WaitStatus = StatusNone
	s.QueryTime = 0

	s.ReadRows = 0
	s.ReadBytes = 0

	s.IndexReadRows = 0
	s.IndexReadBytes = 0
	s.Index = make([]IndexStat, 0)

	s.DataReadRows = 0
	s.DataReadBytes = 0
	s.Data = make([]DataStat, 0)
}

func (s *Stat) TotalQueryRows() float64 {
	var t float64
	for _, i := range s.Index {
		t += i.Time
	}
	for _, d := range s.Data {
		t += d.Time
	}
	return t
}

func (s *Stat) TotalReadRows() int64 {
	var rows int64
	for _, i := range s.Index {
		rows += i.ReadRows
	}
	for _, d := range s.Data {
		rows += d.ReadRows
	}
	return rows
}

func (s *Stat) TotalReadBytes() int64 {
	// a.IndexReadRows+a.DataReadRows < b.IndexReadRows+b.DataReadRows
	var b int64
	for _, i := range s.Index {
		b += i.ReadBytes
	}
	for _, d := range s.Data {
		b += d.ReadBytes
	}
	return b
}

type Sort int8

const (
	SortQTime Sort = iota
	SortRTime
	SortReadRows
	SortIndexReadRows
	SortDataReadRows
	SortQueries
	SortErrors
)

func (s *Sort) Reset(i interface{}) {
	*s = i.(Sort)
}

func (s *Sort) Get() interface{} {
	return s.GetSort()
}

func (s *Sort) GetSort() Sort {
	return *s
}

var sortStrings []string = []string{"qtime", "rtime", "read_rows", "index_read_rows", "data_read_rows", "queries"}

func SortStrings() []string {
	return sortStrings
}

func (s *Sort) Set(value string, _ bool) error {
	switch value {
	case "read_rows":
		*s = SortReadRows
	case "qtime":
		*s = SortQTime
	case "rtime":
		*s = SortRTime
	case "index_read_rows":
		*s = SortIndexReadRows
	case "data_read_rows":
		*s = SortDataReadRows
	case "queries":
		*s = SortQueries
	default:
		return fmt.Errorf("invalid sort %s", value)
	}
	return nil
}

func (s *Sort) String() string {
	return sortStrings[*s]
}

func (s *Sort) Type() string {
	return "sort"
}

func LessStat(a, b *Stat, sortKey Sort) bool {
	switch sortKey {
	case SortQTime:
		if a.QueryTime == b.QueryTime {
			if a.ReadRows == b.ReadRows {
				if a.DataReadRows == b.DataReadRows {
					if a.Points == b.Points {
						if a.Metrics == b.Metrics {
							return a.QueryTime < b.QueryTime
						}
						return a.Metrics < b.Metrics
					}
					return a.Points < b.Points
				}
				return a.DataReadRows < b.DataReadRows
			}
			return a.ReadRows < b.ReadRows
		}
		return a.QueryTime < b.QueryTime
	case SortRTime:
		if a.RequestTime == b.RequestTime {
			if a.ReadRows == b.ReadRows {
				if a.DataReadRows == b.DataReadRows {
					if a.Points == b.Points {
						if a.Metrics == b.Metrics {
							return a.QueryTime < b.QueryTime
						}
						return a.Metrics < b.Metrics
					}
					return a.Points < b.Points
				}
				return a.DataReadRows < b.DataReadRows
			}
			return a.ReadRows < b.ReadRows
		}
		return a.RequestTime < b.RequestTime
	case SortQueries:
		if len(a.Queries) == len(b.Queries) {
			if a.RequestStatus >= http.StatusBadGateway || b.RequestStatus >= http.StatusBadGateway {
				return a.QueryTime < b.QueryTime
			}
			if a.ReadRows == b.ReadRows {
				if a.DataReadRows == b.DataReadRows {
					if a.Points == b.Points {
						if a.Metrics == b.Metrics {
							return a.QueryTime < b.QueryTime
						}
						return a.Metrics < b.Metrics
					}
					return a.Points < b.Points
				}
				return a.DataReadRows < b.DataReadRows
			}
			return a.ReadRows < b.ReadRows
		}
		return len(a.Queries) < len(b.Queries)
	case SortIndexReadRows:
		if a.RequestStatus >= http.StatusBadGateway || b.RequestStatus >= http.StatusBadGateway {
			return a.QueryTime < b.QueryTime
		}
		if a.IndexReadRows == b.IndexReadRows {
			if a.ReadRows == b.ReadRows {
				if a.Points == b.Points {
					if a.Metrics == b.Metrics {
						return a.QueryTime < b.QueryTime
					}
					return a.Metrics < b.Metrics
				}
				return a.Points < b.Points
			}
			return a.ReadRows < b.ReadRows
		}
		return a.IndexReadRows < b.IndexReadRows
	case SortDataReadRows:
		if a.RequestStatus >= http.StatusBadGateway || b.RequestStatus >= http.StatusBadGateway {
			return a.QueryTime < b.QueryTime
		}
		if a.DataReadRows == b.DataReadRows {
			if a.ReadRows == b.ReadRows {
				if a.Points == b.Points {
					if a.Metrics == b.Metrics {
						return a.QueryTime < b.QueryTime
					}
					return a.Metrics < b.Metrics
				}
				return a.Points < b.Points
			}
			return a.ReadRows < b.ReadRows
		}
		return a.DataReadRows < b.DataReadRows
	default:
		if a.RequestStatus >= http.StatusBadGateway || b.RequestStatus >= http.StatusBadGateway {
			return a.QueryTime < b.QueryTime
		}
		if a.ReadRows == b.ReadRows {
			if a.DataReadRows == b.DataReadRows {
				if a.Points == b.Points {
					if a.Metrics == b.Metrics {
						return a.QueryTime < b.QueryTime
					}
					return a.Metrics < b.Metrics
				}
				return a.Points < b.Points
			}
			return a.DataReadRows < b.DataReadRows
		}
		return a.ReadRows < b.ReadRows
	}
}

// func abs(a int64) int64 {
// 	if a < 0 {
// 		return -a
// 	}
// 	return a
// }

func tagQuery(params []string) string {
	var sb strings.Builder
	sb.Grow(128)
	for _, a := range params {
		k, v, _ := strings.Cut(a, "=")
		if k != "" && v != "" {
			switch k {
			case "limit":
				if sb.Len() > 0 {
					sb.WriteString(" ")
				}
				sb.WriteString(a)
			case "expr", "tag", "tagPrefix", "valuePrefix":
				if sb.Len() > 0 {
					sb.WriteString(" ")
				}
				if p, err := url.QueryUnescape(v); err == nil {
					sb.WriteString(k)
					sb.WriteString("='")
					if k, v, ok := strings.Cut(p, "="); ok {
						k = strings.TrimSpace(k)
						v = strings.TrimSpace(v)
						sb.WriteString(k)
						sb.WriteByte('=')
						sb.WriteString(v)
					} else {
						sb.WriteString(p)
					}
					sb.WriteByte('\'')
				} else {
					sb.WriteString(a)
				}
			}
		}
	}
	return sb.String()
}

func metricsFindCacheQuery(cacheKey string) (string, bool) {
	// 1970-02-12;query=test.c*;ts=1674288000
	if strings.HasPrefix(cacheKey, "1970-02-12;query=") {
		cacheKey = cacheKey[17:]
		if query, _, ok := strings.Cut(cacheKey, ";ts="); ok {
			return query, true
		}
	}
	return "", false
}

func LogEntryProcess(logEntry map[string]interface{}, queries map[string]*Stat) string {
	var flushed bool
	request_id, ok := logEntry["request_id"].(string)
	if !ok {
		return ""
	}

	timeStamp, err := time.Parse("2006-01-02T15:04:05.000-0700", logEntry["timestamp"].(string))
	if err != nil {
		fmt.Printf("%v\n", err)
		return ""
	}
	ts := timeStamp.UnixNano()

	v, ok := queries[request_id]
	if ok {
		// leak record, reset
		if ts > v.TimeStamp+240*1e9 || ts < v.TimeStamp {
			v.Reset(ts)
		} else {
			v.TimeStamp = ts
		}
	} else {
		v = &Stat{Id: request_id, TimeStamp: ts}
		queries[request_id] = v
	}

	level := logEntry["level"].(string)

	v.TimeStamp = ts

	logger := logEntry["logger"].(string)
	message := logEntry["message"].(string)

	if message == "query" {
		if logger == "metrics-find" {
			if item, ok := logEntry["query"]; ok {
				q := IndexStat{}
				query := item.(string)

				if level == "ERROR" {
					q.Status = StatusError
					q.Error = readError(logEntry, "error")
				} else {
					q.Status = StatusSuccess
				}

				if item, ok = logEntry["query_id"]; ok {
					q.QueryId = item.(string)
				}

				q.ReadRows, _ = readInt64(logEntry, "read_rows")
				v.IndexReadRows += q.ReadRows
				v.ReadRows += q.ReadRows
				q.ReadBytes, _ = readInt64(logEntry, "read_bytes")
				v.IndexReadBytes += q.ReadBytes
				v.ReadBytes += q.ReadBytes

				q.Time, _ = readFloat64(logEntry, "time")

				if start := strings.Index(query, " FROM "); start > 0 {
					t := query[start+6:]
					if end := strings.Index(t, " "); end > 0 {
						q.Table = t[0:end]
					}
				}
				v.Index = append(v.Index, q)
			}
		} else if logger == "autocomplete" {
			if item, ok := logEntry["query"]; ok {
				q := IndexStat{}
				query := item.(string)

				if level == "ERROR" {
					q.Status = StatusError
					q.Error = readError(logEntry, "error")
				} else {
					q.Status = StatusSuccess
				}

				if item, ok = logEntry["query_id"]; ok {
					q.QueryId = item.(string)
				}

				q.ReadRows, _ = readInt64(logEntry, "read_rows")
				v.IndexReadRows += q.ReadRows
				v.ReadRows += q.ReadRows
				q.ReadBytes, _ = readInt64(logEntry, "read_bytes")
				v.IndexReadBytes += q.ReadBytes
				v.ReadBytes += q.ReadBytes

				q.Time, _ = readFloat64(logEntry, "time")

				start := strings.Index(query, " FROM ")
				if start > 0 {
					t := query[start+6:]
					end := strings.Index(t, " ")
					if end > 0 {
						q.Table = t[0:end]
					}
				}

				t := query[start:]
				if start := strings.Index(t, ") AND (Date >="); start > 0 {
					t = strings.TrimLeft(t[start+14:], " ")
					if t[0] == '\'' && t[11] == '\'' {
						if startDay, err := time.Parse("2006-01-02", t[1:11]); err == nil {
							t = t[11:]
							if end := strings.Index(t, " AND Date <="); end > 0 {
								t = strings.TrimLeft(t[end+12:], " ")
								if t[0] == '\'' && t[11] == '\'' {
									if endDay, err := time.Parse("2006-01-02", t[1:11]); err == nil {
										q.Days = int(endDay.Sub(startDay).Seconds()) / (3600 * 24)
									}
								}
							} else {
								// old query format : without end date
								q.Days = int(timeStamp.Truncate(24*time.Hour).Sub(startDay).Seconds()) / (3600 * 24)
							}
						}
					}
				}
				v.Index = append(v.Index, q)
			}
		} else if logger == "render" {
			// clickhouse query stat
			if item, ok := logEntry["query"]; ok {
				query := item.(string)
				indexQuery := strings.HasPrefix(query, "SELECT Path FROM ")
				if indexQuery {
					// index query
					var q IndexStat

					t := query[17:]
					end := strings.Index(t, " ")
					if end > 0 {
						q.Table = t[0:end]
					}
					if start := strings.Index(t, ") AND (Date >="); start > 0 {
						t = strings.TrimLeft(t[start+14:], " ")
						if t[0] == '\'' && t[11] == '\'' {
							if startDay, err := time.Parse("2006-01-02", t[1:11]); err == nil {
								t = t[11:]
								if end := strings.Index(t, " AND Date <="); end > 0 {
									t = strings.TrimLeft(t[end+12:], " ")
									if t[0] == '\'' && t[11] == '\'' {
										if endDay, err := time.Parse("2006-01-02", t[1:11]); err == nil {
											q.Days = int(endDay.Sub(startDay).Seconds())/(3600*24) + 1
										}
									}
								}
							}
						}
					}

					if level == "ERROR" {
						q.Status = StatusError
						q.Error = readError(logEntry, "error")
					} else {
						q.Status = StatusSuccess
					}

					if item, ok = logEntry["query_id"]; ok {
						q.QueryId = item.(string)
					}

					q.ReadRows, _ = readInt64(logEntry, "read_rows")
					v.IndexReadRows += q.ReadRows
					v.ReadRows += q.ReadRows
					q.ReadBytes, _ = readInt64(logEntry, "read_bytes")
					v.IndexReadBytes += q.ReadBytes
					v.ReadBytes += q.ReadBytes

					q.Time, _ = readFloat64(logEntry, "time")

					v.Index = append(v.Index, q)

				} else {
					// data query
					q := DataStat{}

					if level == "ERROR" {
						q.Status = StatusError
						q.Error = readError(logEntry, "error")
					} else {
						q.Status = StatusSuccess
					}

					if item, ok = logEntry["query_id"]; ok {
						q.QueryId = item.(string)
					}

					q.ReadRows, _ = readInt64(logEntry, "read_rows")
					v.DataReadRows += q.ReadRows
					v.ReadRows += q.ReadRows
					q.ReadBytes, _ = readInt64(logEntry, "read_bytes")
					v.DataReadBytes += q.ReadBytes
					v.ReadBytes += q.ReadBytes

					q.Time, _ = readFloat64(logEntry, "time")

					start := strings.Index(query, " FROM ")
					if start > 0 {
						t := query[start+6:]
						end := strings.Index(t, " ")
						if end > 0 {
							q.Table = t[0:end]
						}
					}

					start = strings.Index(query, "AND (Time >= ")
					if start > 0 {
						t := query[start+13:]
						end := strings.Index(t, " ")
						if end > 0 {
							q.From, _ = strconv.ParseInt(t[0:end], 10, 64)
							// until
							start = strings.Index(t, "AND Time <= ")
							if start > 0 {
								t = t[start+12:]
								end = strings.Index(t, ")")
								if end > 0 {
									q.Until, _ = strconv.ParseInt(t[0:end], 10, 64)
									if q.From > 0 && q.Until > 0 {
										duration := q.Until + 1 - q.From
										q.Days = int((duration)/(3600*24) + 1)
									}
								}
							}
						}
					}

					v.Data = append(v.Data, q)
				}
			}
		}
	} else if level == "INFO" {
		if (logger == "render.pb3parser" && message == "pb3_target") ||
			(logger == "render.json_parser" && message == "json_target") ||
			(logger == "render.form_parser" && message == "target") {

			if item, ok := logEntry["target"]; ok {
				q := Query{
					Query: item.(string),
				}
				q.From, _ = readInt64(logEntry, "from")
				q.Until, _ = readInt64(logEntry, "until")
				if q.From > 0 && q.Until > 0 {
					duration := q.Until - q.From
					q.Days = int((duration)/(3600*24) + 1)

				}
				v.Queries = append(v.Queries, q)
			}

		} else if message == "finder" && (logger == "render" || logger == "metrics-find" || logger == "autocomplete") {
			// find stat
			if item, ok := logEntry["find_cached"]; ok {
				if _, ok := logEntry["ttl"]; ok {
					indexCached := item.(bool)
					metrics, _ := readInt64(logEntry, "metrics")
					v.Metrics += metrics
					if indexCached {
						cacheKey, _ := readString(logEntry, "get_cache")
						if query, ok := metricsFindCacheQuery(cacheKey); ok {
							// metrics/find
							v.Queries = append(v.Queries, Query{Query: query})
						}
						q := IndexStat{}
						from, _ := readInt64(logEntry, "from")
						until, _ := readInt64(logEntry, "until")
						// q.Query, _ = readString(logEntry, "target")
						if until > 0 && from > 0 {
							q.Days = int(until-from)/(3600*24) + 1
						}

						q.Status = StatusCached
						v.Index = append(v.Index, q)
					} else {
						cacheKey, _ := readString(logEntry, "set_cache")
						if query, ok := metricsFindCacheQuery(cacheKey); ok {
							// metrics/find
							v.Queries = append(v.Queries, Query{Query: query})
						}
					}
				}
			}
		} else if logger == "render" && message == "data_parse" {
			v.Points, _ = readInt64(logEntry, "read_points")
			v.Bytes, _ = readInt64(logEntry, "read_bytes")
		} else if logger == "http" && message == "access" {
			// end of query stat
			flushed = true

			v.RequestStatus, _ = readInt64(logEntry, "status")
			v.RequestTime, _ = readFloat64(logEntry, "time")
			v.WaitTime, _ = readFloat64(logEntry, "wait_slot")
			v.QueryTime = v.RequestTime - v.WaitTime
			if waitFail, err := readBool(logEntry, "wait_fail"); waitFail {
				v.WaitStatus = StatusError
			} else if err == nil {
				v.WaitStatus = StatusSuccess
			}

			// if len(v.IndexQuery) == 0 {
			// 	query, _ := readStringSlice(logEntry, "query")
			// 	if len(query) > 0 {
			// 		for _, q := range query {
			// 			v.IndexQuery = append(v.IndexQuery, Query{Query: q})
			// 		}
			// 	}
			// }

			url := logEntry["url"].(string)
			if strings.HasPrefix(url, "/render/?") {
				v.RequestType = "render"
			} else if strings.HasPrefix(url, "/metrics/find/?") {
				v.RequestType = "metrics_find"
			} else if strings.HasPrefix(url, "/tags/autoComplete/values?") {
				v.RequestType = "tag_values"
				query := url[26:]
				params := strings.Split(query, "&")
				v.Queries = []Query{{Query: tagQuery(params)}}
			} else if strings.HasPrefix(url, "/tags/autoComplete/tags?") {
				v.RequestType = "tag_names"
				query := url[24:]
				params := strings.Split(query, "&")
				v.Queries = []Query{{Query: tagQuery(params)}}
			}
		} else if logger == "autocomplete" && message == "finder" {
			// tags autocompleter cache stat
			// if _, ok := logEntry["find_cached"]; ok {
			// indexCached := item.(bool)
			// if indexCached {
			// 	if item, ok := logEntry["get_cache"]; ok {
			// 		v.IndexStatus = StatusCached
			// 		v.Target = item.(string)
			// 	}
			// } else {
			// 	if item, ok := logEntry["set_cache"]; ok {
			// 		v.Target = item.(string)
			// 	}
			// }
			// }
			if v.Metrics == 0 {
				v.Metrics, _ = readInt64(logEntry, "metrics")
			}
			// {
			// 	"level":"INFO","timestamp":"2022-01-13T09:08:56.605+0500",
			// 	"logger":"autocomplete","message":"finder",
			// 	"request_id":"0de2f7c9ef312f906a4f0659c27aee13",
			// 	"get_cache":"tag=namespace ; cluster=prod-cl1","metrics":88,"find_cached":true,"ttl":600
			// }
		}
	}

	if flushed {
		return request_id
	} else {
		return ""
	}
}
