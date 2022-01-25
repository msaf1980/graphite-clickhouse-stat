package stat

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/vjeantet/jodaTime"
)

type Status int8

const (
	StatusNone = iota
	StatusSuccess
	StatusCached
	StatusError
)

var statusStrings []string = []string{" ", "V", "C", "E"}

func (s *Status) String() string {
	return statusStrings[*s]
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

			return v
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

type Stat struct {
	Id     string
	Target string

	TimeStamp int64

	Metrics int64
	Points  int64
	Bytes   int64

	From  int64
	Until int64

	RequestType   string
	RequestTime   float64
	RequestStatus int64

	IndexStatus    Status
	IndexReadRows  int64
	IndexReadBytes int64
	IndexTime      float64
	IndexTable     string
	IndexDays      int
	IndexQueryId   string

	DataStatus    Status
	DataReadRows  int64
	DataReadBytes int64
	DataTime      float64
	DataTable     string
	DataQueryId   string

	Error string
}

func (s *Stat) TotalTime() float64 {
	if s.RequestTime > 0.0 {
		return s.RequestTime
	}
	return s.IndexTime + s.DataTime
}

type Sort int8

const (
	SortTime Sort = iota
	SortReadRows
	SortIndexTime
	SortIndexReadRows
	SortDataTime
	SortDataReadRows
)

var sortStrings []string = []string{"read_rows", "time", "index_read_rows", "index_time", "data_read_rows", "data_time"}

func SortStrings() []string {
	return sortStrings
}

func (s *Sort) Set(value string) error {
	switch value {
	case "read_rows":
		*s = SortReadRows
	case "time":
		*s = SortTime
	case "index_read_rows":
		*s = SortIndexReadRows
	case "index_time":
		*s = SortIndexTime
	case "data_read_rows":
		*s = SortDataReadRows
	case "data_time":
		*s = SortDataTime
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
	case SortTime:
		if a.TotalTime() == b.TotalTime() {
			return a.IndexReadRows+a.DataReadRows < b.IndexReadRows+b.DataReadRows
		}
		return a.TotalTime() < b.TotalTime()
	case SortIndexReadRows:
		if a.IndexReadRows == b.IndexReadRows || a.IndexReadRows == 0 || b.IndexReadRows == 0 {
			return a.IndexTime < b.IndexTime
		}
		return a.IndexReadRows < b.IndexReadRows
	case SortIndexTime:
		if a.IndexTime == b.IndexTime {
			return a.IndexReadRows < b.IndexReadRows
		}
		return a.IndexTime < b.IndexTime
	case SortDataReadRows:
		if a.DataReadRows == b.DataReadRows || a.DataReadRows == 0 || b.DataReadRows == 0 {
			return a.DataTime < b.DataTime
		}
		return a.DataReadRows < b.DataReadRows
	case SortDataTime:
		if a.DataTime == b.DataTime {
			return a.DataReadRows < b.DataReadRows
		}
		return a.DataTime < b.DataTime
	default:
		aReadRows := a.IndexReadRows + a.DataReadRows
		bReadRows := b.IndexReadRows + b.DataReadRows
		if a.IndexReadRows == 0 || a.DataReadRows == 0 || b.IndexReadRows == 0 || b.DataReadRows == 0 || aReadRows == bReadRows {
			return a.TotalTime() < b.TotalTime()
		} else {
			return aReadRows < bReadRows
		}
	}
}

func LogEntryProcess(logEntry map[string]interface{}, queries map[string]*Stat) string {
	var flushed bool
	request_id := logEntry["request_id"].(string)

	v, ok := queries[request_id]
	if !ok {
		v = &Stat{Id: request_id}
		queries[request_id] = v
	}

	level := logEntry["level"].(string)

	timeStamp, err := jodaTime.Parse("yyyy-MM-ddTHH:mm:ss.SZ", logEntry["timestamp"].(string))
	if err != nil {
		fmt.Printf("%v", err)
		return ""
	}
	v.TimeStamp = timeStamp.UnixNano()

	logger := logEntry["logger"].(string)
	message := logEntry["message"].(string)

	if message == "query" {
		if logger == "metrics-find" {
			if item, ok := logEntry["query"]; ok {
				query := item.(string)

				if level == "ERROR" {
					v.IndexStatus = StatusError
					v.Error = readError(logEntry, "error")
				} else {
					v.IndexStatus = StatusSuccess
				}

				if item, ok = logEntry["query_id"]; ok {
					v.IndexQueryId = item.(string)
				}

				v.IndexReadRows, _ = readInt64(logEntry, "read_rows")
				v.IndexReadBytes, _ = readInt64(logEntry, "read_bytes")

				v.IndexTime, _ = readFloat64(logEntry, "time")

				start := strings.Index(query, " FROM ")
				if start > 0 {
					t := query[start+6:]
					end := strings.Index(t, " ")
					if end > 0 {
						v.IndexTable = t[0:end]
					}
				}
			}
		} else if logger == "autocomplete" {
			if item, ok := logEntry["query"]; ok {
				query := item.(string)

				if level == "ERROR" {
					v.IndexStatus = StatusError
					v.Error = readError(logEntry, "error")
				} else {
					v.IndexStatus = StatusSuccess
				}

				if item, ok = logEntry["query_id"]; ok {
					v.IndexQueryId = item.(string)
				}

				v.IndexReadRows, _ = readInt64(logEntry, "read_rows")
				v.IndexReadBytes, _ = readInt64(logEntry, "read_bytes")

				v.IndexTime, _ = readFloat64(logEntry, "time")

				start := strings.Index(query, " FROM ")
				if start > 0 {
					t := query[start+6:]
					end := strings.Index(t, " ")
					if end > 0 {
						v.IndexTable = t[0:end]
					}
				}

				t := query
				if start := strings.Index(t, ") AND (Date >="); start > 0 {
					t = strings.TrimLeft(t[start+14:], " ")
					if t[0] == '\'' && t[11] == '\'' {
						if startDay, err := jodaTime.Parse("YYYY-MM-dd", t[1:11]); err == nil {
							t = t[11:]
							if end := strings.Index(t, " AND Date <="); end > 0 {
								t = strings.TrimLeft(t[end+12:], " ")
								if t[0] == '\'' && t[11] == '\'' {
									if endDay, err := jodaTime.Parse("YYYY-MM-dd", t[1:11]); err == nil {
										v.IndexDays = int(endDay.Sub(startDay).Seconds()) / (3600 * 34)
									}
								}
							} else {
								// old query format : without end date
								v.IndexDays = int(timeStamp.Truncate(24*time.Hour).Sub(startDay).Seconds()) / (3600 * 34)
							}
						}
					}
				}
			}
		} else if logger == "render" {
			// clickhouse query stat
			if item, ok := logEntry["query"]; ok {
				var queryError bool

				if level == "ERROR" {
					queryError = true
					v.Error = readError(logEntry, "error")
				}

				query := item.(string)
				indexQuery := strings.HasPrefix(query, "SELECT Path FROM ")
				if indexQuery {
					if queryError {
						v.IndexStatus = StatusError
					} else {
						v.IndexStatus = StatusSuccess
					}
					if item, ok = logEntry["query_id"]; ok {
						v.IndexQueryId = item.(string)
					}

					v.IndexReadRows, _ = readInt64(logEntry, "read_rows")
					v.IndexReadBytes, _ = readInt64(logEntry, "read_bytes")

					v.IndexTime, _ = readFloat64(logEntry, "time")

					t := query[17:]
					end := strings.Index(t, " ")
					if end > 0 {
						v.IndexTable = t[0:end]
					}
					if start := strings.Index(t, ") AND (Date >="); start > 0 {
						t = strings.TrimLeft(t[start+14:], " ")
						if t[0] == '\'' && t[11] == '\'' {
							if startDay, err := jodaTime.Parse("YYYY-MM-dd", t[1:11]); err == nil {
								t = t[11:]
								if end := strings.Index(t, " AND Date <="); end > 0 {
									t = strings.TrimLeft(t[end+12:], " ")
									if t[0] == '\'' && t[11] == '\'' {
										if endDay, err := jodaTime.Parse("YYYY-MM-dd", t[1:11]); err == nil {
											v.IndexDays = int(endDay.Sub(startDay).Seconds())/(3600*34) + 1
										}
									}
								}
							}
						}
					}
					if v.IndexDays == 0 {
						v.IndexDays = int(v.Until-v.From)/(3600*34) + 1
					}
				} else {
					if queryError {
						v.DataStatus = StatusError
					} else {
						v.DataStatus = StatusSuccess
					}
					if item, ok = logEntry["query_id"]; ok {
						v.DataQueryId = item.(string)
					}
					// from
					if v.From == 0 && v.Until == 0 {
						start := strings.Index(query, "AND (Time >= ")
						if start > 0 {
							t := query[start+13:]
							end := strings.Index(t, " ")
							if end > 0 {
								v.From, _ = strconv.ParseInt(t[0:end], 10, 64)

								// until
								start = strings.Index(t, "AND Time <= ")
								if start > 0 {
									t = t[start+12:]
									end = strings.Index(t, ")")
									if end > 0 {
										v.Until, _ = strconv.ParseInt(t[0:end], 10, 64)
									}
								}
							}
						}
					}

					v.DataReadRows, _ = readInt64(logEntry, "read_rows")
					v.DataReadBytes, _ = readInt64(logEntry, "read_bytes")

					v.DataTime, _ = readFloat64(logEntry, "time")

					start := strings.Index(query, " FROM ")
					if start > 0 {
						t := query[start+6:]
						end := strings.Index(t, " ")
						if end > 0 {
							v.DataTable = t[0:end]
						}
					}
				}
			}
		}
	} else if level == "INFO" {
		if logger == "render.pb3parser" && message == "pb3_target" {
			if item, ok := logEntry["target"]; ok {
				v.Target = item.(string)
			}
			v.From, _ = readInt64(logEntry, "from")
			v.Until, _ = readInt64(logEntry, "until")
		} else if message == "finder" && (logger == "render" || logger == "metrics-find" || logger == "autocomplete") {
			// find stat
			if item, ok := logEntry["find_cached"]; ok {
				indexCached := item.(bool)
				if indexCached {
					if item, ok := logEntry["get_cache"]; ok {
						v.IndexStatus = StatusCached
						if len(v.Target) == 0 {
							v.Target = item.(string)
						}
					}
				} else if len(v.Target) == 0 {
					if item, ok := logEntry["set_cache"]; ok {
						v.Target = item.(string)
					}
				}
				if v.Metrics == 0 {
					v.Metrics, _ = readInt64(logEntry, "metrics")
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

			url := logEntry["url"].(string)
			if strings.HasPrefix(url, "/render/?") {
				v.RequestType = "render"
			} else if strings.HasPrefix(url, "/metrics/find/?") {
				v.RequestType = "metrics_find"
			} else if strings.HasPrefix(url, "/tags/autoComplete/values?") {
				v.RequestType = "tag_values"
			} else if strings.HasPrefix(url, "/tags/autoComplete/tags?") {
				v.RequestType = "autocomplete"
			}
		} else if logger == "autocomplete" && message == "finder" {
			// tags autocompleter cache stat
			if item, ok := logEntry["find_cached"]; ok {
				indexCached := item.(bool)
				if indexCached {
					if item, ok := logEntry["get_cache"]; ok {
						v.IndexStatus = StatusCached
						v.Target = item.(string)
					}
				} else {
					if item, ok := logEntry["set_cache"]; ok {
						v.Target = item.(string)
					}
				}
				if v.Metrics == 0 {
					v.Metrics, _ = readInt64(logEntry, "metrics")
				}
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
