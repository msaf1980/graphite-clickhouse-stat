package stat

import (
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
)

func Test_LogEntryProcess(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
		queries map[string]*Stat
	}{
		{
			name: "render TEST.app.* (set cache)",
			entries: []string{
				`{"level":"INFO","timestamp":"2022-01-12T20:27:06.388+0300","logger":"render.pb3parser","message":"pb3_target","request_id":"a0a47c58b609722954475cd58e10d39c","carbonapi_uuid":"de5f746d-6261-4349-9cca-4bb4608290b6","from":1642007700,"until":1642008429,"maxDataPoints":586,"target":"TEST.app.*"}`,
				`{"level":"INFO","timestamp":"2022-01-12T20:27:06.388+0300","logger":"render","message":"query","request_id":"a0a47c58b609722954475cd58e10d39c","carbonapi_uuid":"de5f746d-6261-4349-9cca-4bb4608290b6","query":"SELECT Path FROM graphite_index_short WHERE ((Level=3) AND (Path LIKE 'TEST.app.%' AND match(Path, '^TEST[.]app[.]([^.]*?)$'))) AND (Date >= '2022-01-12' AND Date <= '2022-01-12') GROUP BY Path FORMAT TabSeparatedRaw","total_rows_to_read":"102960","read_rows":"102960","read_bytes":"13749735","written_rows":"0","written_bytes":"0","query_id":"16C93F1B02F29731","time":0.007430587}`,
				`{"level":"INFO","timestamp":"2022-01-12T20:27:06.388+0300","logger":"render","message":"finder","request_id":"a0a47c58b609722954475cd58e10d39c","carbonapi_uuid":"de5f746d-6261-4349-9cca-4bb4608290b6","set_cache":"TEST.app.*","timestamp_cached":"2022-01-12T20:27:00.000+0300","metrics":3,"find_cached":false,"ttl":60}`,
				`{"level":"INFO","timestamp":"2022-01-12T20:27:06.388+0300","logger":"render","message":"finder","request_id":"a0a47c58b609722954475cd58e10d39c","carbonapi_uuid":"de5f746d-6261-4349-9cca-4bb4608290b6","metrics":3,"find_cached":false}`,
				`{"level":"INFO","timestamp":"2022-01-12T20:27:06.407+0300","logger":"render","message":"query","request_id":"a0a47c58b609722954475cd58e10d39c","carbonapi_uuid":"de5f746d-6261-4349-9cca-4bb4608290b6","query":"WITH anyResample(1642007700, 1642008429, 10)(toUInt32(intDiv(Time, 10)*10), Time) AS mask SELECT Path, arrayFilter(m->m!=0, mask) AS times, arrayFilter((v,m)->m!=0, avgResample(1642007700, 1642008429, 10)(Value, Time), mask) AS values FROM graphite_short PREWHERE Date >= toDate(1642007700) AND Date <= toDate(1642008429) WHERE (Path in metrics_list) AND (Time >= 1642007700 AND Time <= 1642008429) GROUP BY Path FORMAT RowBinary","read_rows":"446464","read_bytes":"58850683","written_rows":"0","written_bytes":"0","total_rows_to_read":"446464","query_id":"16C93F1B02F29735","time":0.018982262}`,
				`{"level":"INFO","timestamp":"2022-01-12T20:27:06.407+0300","logger":"render","message":"data_parse","request_id":"a0a47c58b609722954475cd58e10d39c","carbonapi_uuid":"de5f746d-6261-4349-9cca-4bb4608290b6","read_bytes":2958,"read_points":216,"runtime":"203.32µs","runtime_ns":0.00020332}`,
				`{"level":"INFO","timestamp":"2022-01-12T20:27:06.407+0300","logger":"http","message":"access","request_id":"a0a47c58b609722954475cd58e10d39c","carbonapi_uuid":"de5f746d-6261-4349-9cca-4bb4608290b6","time":0.027103758,"method":"GET","url":"/render/?format=carbonapi_v3_pb","peer":"127.0.0.1:9090","client":"","status":200,"find_cached":false}`,
			},
			queries: map[string]*Stat{
				"a0a47c58b609722954475cd58e10d39c": {
					Id:             "a0a47c58b609722954475cd58e10d39c",
					Target:         "TEST.app.*",
					TimeStamp:      1642008426407000000,
					Metrics:        3,
					Points:         216,
					Bytes:          2958,
					RequestType:    "render",
					RequestTime:    0.027103758,
					From:           1642007700,
					Until:          1642008429,
					RequestStatus:  200,
					IndexStatus:    StatusSuccess,
					IndexReadRows:  102960,
					IndexReadBytes: 13749735,
					IndexTime:      0.007430587,
					IndexQueryId:   "16C93F1B02F29731",
					IndexTable:     "graphite_index_short",
					IndexDays:      1,
					DataStatus:     StatusSuccess,
					DataReadRows:   446464,
					DataReadBytes:  58850683,
					DataTime:       0.018982262,
					DataQueryId:    "16C93F1B02F29735",
					DataTable:      "graphite_short",
				},
			},
		},
		{
			name: "metrics_find TEST.*",
			entries: []string{
				`{"level":"INFO","timestamp":"2022-01-25T04:20:22.307+0300","logger":"metrics-find","message":"query","request_id":"078bf7e432aa271e821c7b4bc7a265e3","carbonapi_uuid":"3ed8b483-4ea7-47f2-a043-44b5d84be6ce","query":"SELECT Path FROM graphite_indexd WHERE ((Level=20004) AND (Path LIKE 'DevOps.carbon.relays.%' AND match(Path, '^DevOps[.]carbon[.]relays[.](sd2-graphite-r2|bst-graphite-r2|bst-graphite-r3|dtl-graphite-r2|dtl-graphite-r3|xlt-graphite-r1)[.]?$'))) AND (Date='1970-02-12') GROUP BY Path FORMAT TabSeparatedRaw","read_bytes":"14504888","written_rows":"0","written_bytes":"0","total_rows_to_read":"211348","read_rows":"211348","query_id":"16C93F1B109EBF3C","time":0.20100828}`,
				`{"level":"INFO","timestamp":"2022-01-25T04:20:22.307+0300","logger":"metrics-find","message":"finder","request_id":"078bf7e432aa271e821c7b4bc7a265e3","carbonapi_uuid":"3ed8b483-4ea7-47f2-a043-44b5d84be6ce","set_cache":"TEST.*","metrics":7,"find_cached":false,"ttl":300}`,
				`{"level":"INFO","timestamp":"2022-01-25T04:20:22.307+0300","logger":"http","message":"access","request_id":"078bf7e432aa271e821c7b4bc7a265e3","carbonapi_uuid":"3ed8b483-4ea7-47f2-a043-44b5d84be6ce","time":0.201596796,"method":"GET","url":"/metrics/find/?format=carbonapi_v3_pb","peer":"192.168.85.127:42272","client":"","status":200,"find_cached":false}`,
			},
			queries: map[string]*Stat{
				"078bf7e432aa271e821c7b4bc7a265e3": {
					Id:             "078bf7e432aa271e821c7b4bc7a265e3",
					Target:         "TEST.*",
					TimeStamp:      1643073622307000000,
					Metrics:        7,
					RequestType:    "metrics_find",
					RequestTime:    0.201596796,
					RequestStatus:  200,
					IndexStatus:    StatusSuccess,
					IndexDays:      0,
					IndexReadRows:  211348,
					IndexReadBytes: 14504888,
					IndexTime:      0.20100828,
					IndexTable:     "graphite_indexd",
					IndexQueryId:   "16C93F1B109EBF3C",
				},
			},
		},
		{
			name: "tags_autocomplete tag=environment project=TEST project=WEB (set cache)",
			entries: []string{
				`{"level":"INFO","timestamp":"2022-01-13T04:56:36.560+0300","logger":"autocomplete","message":"query","request_id":"fe9228290bc2a4a989b8735b16a9cec8","carbonapi_uuid":"043cd60d-e597-4687-9fe1-ce7a4edffcda","query":"SELECT substr(arrayJoin(Tags), 13) AS value FROM graphite_tags  WHERE ((((Tag1='project=TEST', Tags))) AND (arrayJoin(Tags) LIKE 'environment=%')) AND (Date >= '2022-01-06') GROUP BY value ORDER BY value LIMIT 10000","read_rows":"2692558","read_bytes":"777909148","written_rows":"0","written_bytes":"0","total_rows_to_read":"2692558","query_id":"16C93F1ADD516808","time":0.854434785}`,
				`{"level":"INFO","timestamp":"2022-01-13T09:08:56.605+0500","logger":"autocomplete","message":"finder","request_id":"fe9228290bc2a4a989b8735b16a9cec8","carbonapi_uuid":"043cd60d-e597-4687-9fe1-ce7a4edffcda","set_cache":"tag=environment ; project=TEST","metrics":88,"find_cached":false,"ttl":600}`,
				`{"level":"INFO","timestamp":"2022-01-13T04:56:36.561+0300","logger":"http","message":"access","request_id":"fe9228290bc2a4a989b8735b16a9cec8","carbonapi_uuid":"043cd60d-e597-4687-9fe1-ce7a4edffcda","time":0.856165462,"method":"GET","url":"/tags/autoComplete/values?expr==project+%3D+TEST&from=1642037194&limit=10000&tag=environment&until=1642038996","peer":"192.168.173.10:48012","client":"","status":200,"find_cached":false}`,
			},
			queries: map[string]*Stat{
				"fe9228290bc2a4a989b8735b16a9cec8": {
					Id:             "fe9228290bc2a4a989b8735b16a9cec8",
					Target:         "tag=environment ; project=TEST",
					TimeStamp:      1642038996561000000,
					Metrics:        88,
					RequestType:    "tag_values",
					RequestTime:    0.856165462,
					RequestStatus:  200,
					IndexStatus:    StatusSuccess,
					IndexDays:      4,
					IndexReadRows:  2692558,
					IndexReadBytes: 777909148,
					IndexTime:      0.854434785,
					IndexTable:     "graphite_tags",
					IndexQueryId:   "16C93F1ADD516808",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queries := make(map[string]*Stat)

			for _, entry := range tt.entries {
				var logEntry map[string]interface{}
				err := json.Unmarshal([]byte(entry), &logEntry)
				if err != nil {
					t.Fatalf("%v: %s", err, entry)
				}
				LogEntryProcess(logEntry, queries)
			}

			assert.Equal(t, tt.queries, queries)
		})
	}
}
