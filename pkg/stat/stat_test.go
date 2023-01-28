package stat

import (
	"reflect"
	"testing"

	"github.com/goccy/go-json"
	"github.com/google/go-cmp/cmp"
)

func Test_LogEntryProcess(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
		queries map[string]*Stat
	}{
		{
			name: "render test.a",
			entries: []string{
				`{"level":"INFO","timestamp":"2023-01-21T13:05:43.290+0500","logger":"render.pb3parser","message":"pb3_target","request_id":"1f72e822bed05bebd97a9bdcc4654f1a","from":1674288223,"until":1674288343,"maxDataPoints":0,"target":"test.a"}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:05:43.510+0500","logger":"render","message":"query","request_id":"1f72e822bed05bebd97a9bdcc4654f1a","query":"SELECT Path FROM graphite_indexd WHERE ((Level=8) AND (Path IN ('test.a'))) AND (Date >='2023-01-21' AND Date <= '2023-01-21') GROUP BY Path FORMAT TabSeparatedRaw","read_rows":"241436","read_bytes":"31416887","written_rows":"0","written_bytes":"0","total_rows_to_read":"241436","query_id":"1f72e822bed05bebd97a9bdcc4654f1a::1390f060ca3d959d","time":0.219432977}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:05:43.510+0500","logger":"render","message":"finder","request_id":"1f72e822bed05bebd97a9bdcc4654f1a","set_cache":"2023-01-21;2023-01-21;test.a;ttl=60","timestamp_cached":"2023-01-21T13:05:00.000+0500","metrics":1,"find_cached":false,"ttl":"60","from":1674288223,"until":1674288343,"target":"test.a"}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:05:43.510+0500","logger":"render","message":"finder","request_id":"1f72e822bed05bebd97a9bdcc4654f1a","metrics":1,"find_cached":false}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:05:43.772+0500","logger":"render","message":"query","request_id":"1f72e822bed05bebd97a9bdcc4654f1a","query":"WITH anyResample(1674288230, 1674288349, 10)(toUInt32(intDiv(Time, 10)*10), Time) AS mask SELECT Path, arrayFilter(m->m!=0, mask) AS times, arrayFilter((v,m)->m!=0, avgResample(1674288230, 1674288349, 10)(Value, Time), mask) AS values FROM graphite_reversed PREWHERE Date >= '2023-01-21' AND Date <= '2023-01-21' WHERE (Path in metrics_list) AND (Time >= 1674288230 AND Time <= 1674288349) GROUP BY Path FORMAT RowBinary","read_rows":"1228804","read_bytes":"164970948","written_rows":"0","written_bytes":"0","total_rows_to_read":"1228800","query_id":"1f72e822bed05bebd97a9bdcc4654f1a::1b87069be1c53ee2","time":0.261669254}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:05:43.772+0500","logger":"render","message":"data_parse","request_id":"1f72e822bed05bebd97a9bdcc4654f1a","read_bytes":148,"read_points":4,"runtime":"39.481364ms","runtime_ns":0.039481364}`,
				`{"level":"DEBUG","timestamp":"2023-01-21T13:05:43.773+0500","logger":"render","message":"reply","request_id":"1f72e822bed05bebd97a9bdcc4654f1a","runtime":"63.018µs","runtime_ns":0.000063018}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:05:43.773+0500","logger":"http","message":"access","request_id":"1f72e822bed05bebd97a9bdcc4654f1a","time":0.482252576,"wait_slot":0,"wait_fail":false,"method":"GET","url":"/render/?format=carbonapi_v3_pb","peer":"127.0.0.1:40354","client":"","status":200,"find_cached":false}`,
			},
			queries: map[string]*Stat{
				"1f72e822bed05bebd97a9bdcc4654f1a": {
					RequestType: "render", Id: "1f72e822bed05bebd97a9bdcc4654f1a",
					TimeStamp:     1674288343773000000,
					Metrics:       1,
					Points:        4,
					Bytes:         148,
					RequestStatus: 200, RequestTime: 0.482252576, QueryTime: 0.482252576,
					WaitStatus: StatusSuccess,
					ReadRows:   241436 + 1228804, ReadBytes: 31416887 + 164970948,
					Queries:       []Query{{Query: "test.a", Days: 1, From: 1674288223, Until: 1674288343}},
					IndexReadRows: 241436, IndexReadBytes: 31416887,
					Index: []IndexStat{
						{
							Status: 1, Time: 0.219432977,
							ReadRows: 241436, ReadBytes: 31416887,
							Table:   "graphite_indexd",
							QueryId: "1f72e822bed05bebd97a9bdcc4654f1a::1390f060ca3d959d",
							Days:    1,
						},
					},
					DataReadRows: 1228804, DataReadBytes: 164970948,
					Data: []DataStat{
						{
							Status: 1, Time: 0.261669254,
							ReadRows: 1228804, ReadBytes: 164970948,
							Table:   "graphite_reversed",
							QueryId: "1f72e822bed05bebd97a9bdcc4654f1a::1b87069be1c53ee2",
							Days:    1, From: 1674288230, Until: 1674288349,
						},
					},
				},
			},
		},
		{
			name: "render test.a (cached)",
			entries: []string{
				`{"level":"INFO","timestamp":"2023-01-21T13:05:50.050+0500","logger":"render.pb3parser","message":"pb3_target","request_id":"3dba74b5575b2bc262bab3029c1b34fd","from":1674288230,"until":1674288350,"maxDataPoints":0,"target":"test.a"}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:05:50.050+0500","logger":"render","message":"finder","request_id":"3dba74b5575b2bc262bab3029c1b34fd","get_cache":"2023-01-21;2023-01-21;test.a;ttl=60","timestamp_cached":"2023-01-21T13:05:00.000+0500","metrics":1,"find_cached":true,"ttl":"60","from":1674288230,"until":1674288350,"target":"test.a"}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:05:50.050+0500","logger":"render","message":"finder","request_id":"3dba74b5575b2bc262bab3029c1b34fd","metrics":1,"find_cached":true}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:05:50.371+0500","logger":"render","message":"query","request_id":"3dba74b5575b2bc262bab3029c1b34fd","query":"WITH anyResample(1674288230, 1674288359, 10)(toUInt32(intDiv(Time, 10)*10), Time) AS mask SELECT Path, arrayFilter(m->m!=0, mask) AS times, arrayFilter((v,m)->m!=0, avgResample(1674288230, 1674288359, 10)(Value, Time), mask) AS values FROM graphite_reversed PREWHERE Date >= '2023-01-21' AND Date <= '2023-01-21' WHERE (Path in metrics_list) AND (Time >= 1674288230 AND Time <= 1674288359) GROUP BY Path FORMAT RowBinary","read_rows":"1228804","read_bytes":"164245923","written_rows":"0","written_bytes":"0","total_rows_to_read":"1228800","query_id":"3dba74b5575b2bc262bab3029c1b34fd::983c8741c6dc02fc","time":0.320501832}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:05:50.371+0500","logger":"render","message":"data_parse","request_id":"3dba74b5575b2bc262bab3029c1b34fd","read_bytes":160,"read_points":5,"runtime":"40.77259ms","runtime_ns":0.04077259}`,
				`{"level":"DEBUG","timestamp":"2023-01-21T13:05:50.374+0500","logger":"render","message":"reply","request_id":"3dba74b5575b2bc262bab3029c1b34fd","runtime":"2.32899ms","runtime_ns":0.00232899}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:05:50.374+0500","logger":"http","message":"access","request_id":"3dba74b5575b2bc262bab3029c1b34fd","time":0.323721465,"wait_slot":0,"wait_fail":false,"method":"GET","url":"/render/?format=carbonapi_v3_pb","peer":"127.0.0.1:44570","client":"","status":200,"find_cached":true}`,
			},
			queries: map[string]*Stat{
				"3dba74b5575b2bc262bab3029c1b34fd": {
					Id:        "3dba74b5575b2bc262bab3029c1b34fd",
					TimeStamp: 1674288350374000000,
					Metrics:   1, Points: 5, Bytes: 160,
					RequestType:   "render",
					RequestStatus: 200, RequestTime: 0.323721465, QueryTime: 0.323721465,
					WaitStatus: StatusSuccess,
					ReadRows:   1228804, ReadBytes: 164245923,
					Queries:      []Query{{Query: "test.a", Days: 1, From: 1674288230, Until: 1674288350}},
					Index:        []IndexStat{{Status: StatusCached, Days: 1}},
					DataReadRows: 1228804, DataReadBytes: 164245923,
					Data: []DataStat{
						{
							Time: 0.320501832, Status: 1,
							ReadRows: 1228804, ReadBytes: 164245923,
							Table:   "graphite_reversed",
							QueryId: "3dba74b5575b2bc262bab3029c1b34fd::983c8741c6dc02fc",
							Days:    1, From: 1674288230, Until: 1674288359,
						},
					},
				},
			},
		},
		{
			name: "render test.a (cached) test.b",
			entries: []string{
				`{"level":"INFO","timestamp":"2023-01-21T14:39:09.928+0500","logger":"render.pb3parser","message":"pb3_target","request_id":"3aa5cd1be020f8924438ca9969718a6c","from":1674293829,"until":1674293949,"maxDataPoints":0,"target":"test.a"}`,
				`{"level":"INFO","timestamp":"2023-01-21T14:39:09.928+0500","logger":"render.pb3parser","message":"pb3_target","request_id":"3aa5cd1be020f8924438ca9969718a6c","from":1674293829,"until":1674293949,"maxDataPoints":0,"target":"test.b"}`,
				`{"level":"INFO","timestamp":"2023-01-21T14:39:09.928+0500","logger":"render","message":"finder","request_id":"3aa5cd1be020f8924438ca9969718a6c","get_cache":"2023-01-21;2023-01-21;test.a;ttl=60","timestamp_cached":"2023-01-21T14:39:00.000+0500","metrics":1,"find_cached":true,"ttl":"60","from":1674293829,"until":1674293949,"target":"test.a"}`,
				`{"level":"INFO","timestamp":"2023-01-21T14:39:10.034+0500","logger":"render","message":"query","request_id":"3aa5cd1be020f8924438ca9969718a6c","query":"SELECT Path FROM graphite_indexd WHERE ((Level=2) AND (Path IN ('test.b','test.b.'))) AND (Date >='2023-01-21' AND Date <= '2023-01-21') GROUP BY Path FORMAT TabSeparatedRaw","read_rows":"40960","read_bytes":"3442149","written_rows":"0","written_bytes":"0","total_rows_to_read":"40960","query_id":"3aa5cd1be020f8924438ca9969718a6c::92c348bfbb8c60c6","time":0.105761861}`,
				`{"level":"INFO","timestamp":"2023-01-21T14:39:10.034+0500","logger":"render","message":"finder","request_id":"3aa5cd1be020f8924438ca9969718a6c","set_cache":"2023-01-21;2023-01-21;test.b;ttl=60","timestamp_cached":"2023-01-21T14:39:00.000+0500","metrics":1,"find_cached":false,"ttl":"60"}`,
				`{"level":"INFO","timestamp":"2023-01-21T14:39:10.034+0500","logger":"render","message":"finder","request_id":"3aa5cd1be020f8924438ca9969718a6c","metrics":2,"find_cached":true}`,
				`{"level":"INFO","timestamp":"2023-01-21T14:39:10.263+0500","logger":"render","message":"query","request_id":"3aa5cd1be020f8924438ca9969718a6c","query":"WITH anyResample(1674293830, 1674293949, 10)(toUInt32(intDiv(Time, 10)*10), Time) AS mask SELECT Path, arrayFilter(m->m!=0, mask) AS times, arrayFilter((v,m)->m!=0, avgResample(1674293830, 1674293949, 10)(Value, Time), mask) AS values FROM graphite_reversed PREWHERE Date >= '2023-01-21' AND Date <= '2023-01-21' WHERE (Path in metrics_list) AND (Time >= 1674293830 AND Time <= 1674293949) GROUP BY Path FORMAT RowBinary","read_rows":"884740","read_bytes":"120051188","written_rows":"0","written_bytes":"0","total_rows_to_read":"884736","query_id":"3aa5cd1be020f8924438ca9969718a6c::098a06fd021c538f","time":0.228199743}`,
				`{"level":"INFO","timestamp":"2023-01-21T14:39:10.263+0500","logger":"render","message":"data_parse","request_id":"3aa5cd1be020f8924438ca9969718a6c","read_bytes":112,"read_points":1,"runtime":"40.050358ms","runtime_ns":0.040050358}`,
				`{"level":"DEBUG","timestamp":"2023-01-21T14:39:10.263+0500","logger":"render","message":"reply","request_id":"3aa5cd1be020f8924438ca9969718a6c","runtime":"53.584µs","runtime_ns":0.000053584}`,
				`{"level":"INFO","timestamp":"2023-01-21T14:39:10.263+0500","logger":"http","message":"access","request_id":"3aa5cd1be020f8924438ca9969718a6c","time":0.334478006,"wait_slot":0,"wait_fail":false,"method":"GET","url":"/render/?format=carbonapi_v3_pb","peer":"127.0.0.1:39260","client":"","status":200,"find_cached":true}`,
			},
			queries: map[string]*Stat{
				"3aa5cd1be020f8924438ca9969718a6c": {
					RequestType: "render", Id: "3aa5cd1be020f8924438ca9969718a6c",
					TimeStamp: 1674293950263000000,
					Metrics:   2, Points: 1, Bytes: 112,
					RequestStatus: 200, RequestTime: 0.334478006, QueryTime: 0.334478006,
					WaitStatus: StatusSuccess,
					ReadRows:   40960 + 884740,
					ReadBytes:  3442149 + 120051188,
					Queries: []Query{
						{Query: "test.a", Days: 1, From: 1674293829, Until: 1674293949},
						{Query: "test.b", Days: 1, From: 1674293829, Until: 1674293949},
					},
					IndexReadRows: 40960, IndexReadBytes: 3442149,
					Index: []IndexStat{
						{Status: StatusCached, Days: 1},
						{
							Time: 0.105761861, Status: StatusSuccess,
							ReadRows: 40960, ReadBytes: 3442149,
							Table:   "graphite_indexd",
							QueryId: "3aa5cd1be020f8924438ca9969718a6c::92c348bfbb8c60c6",
							Days:    1,
						},
					},
					DataReadRows: 884740, DataReadBytes: 120051188,
					Data: []DataStat{
						{
							Time: 0.228199743, Status: 1,
							ReadRows: 884740, ReadBytes: 120051188,
							Table:   "graphite_reversed",
							QueryId: "3aa5cd1be020f8924438ca9969718a6c::098a06fd021c538f",
							Days:    1, From: 1674293830, Until: 1674293949,
						},
					},
				},
			},
		},
		{
			name: "/metrics/find test.c*",
			entries: []string{
				`{"level":"INFO","timestamp":"2023-01-21T13:06:20.528+0500","logger":"metrics-find","message":"query","request_id":"fd3e9fd09a92bc3b7fb0d597f901e953","query":"SELECT Path FROM graphite_indexd WHERE ((Level=20002) AND (Path LIKE 'test.c%')) AND (Date='1970-02-12') GROUP BY Path FORMAT TabSeparatedRaw","read_rows":"413049","read_bytes":"24262486","written_rows":"0","written_bytes":"0","total_rows_to_read":"413049","query_id":"fd3e9fd09a92bc3b7fb0d597f901e953::9c3fc3cb99436f1b","time":0.174105795}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:06:20.528+0500","logger":"metrics-find","message":"finder","request_id":"fd3e9fd09a92bc3b7fb0d597f901e953","set_cache":"1970-02-12;query=test.c*;ts=1674288000","metrics":6,"find_cached":false,"ttl":600,"query":["test.c*"]}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:06:20.528+0500","logger":"http","message":"access","request_id":"fd3e9fd09a92bc3b7fb0d597f901e953","time":0.174497662,"wait_slot":0,"wait_fail":false,"method":"GET","url":"/metrics/find/?format=carbonapi_v3_pb&query=test.c%2A","peer":"127.0.0.1:39814","client":"","status":200,"find_cached":false}`,
			},
			queries: map[string]*Stat{
				"fd3e9fd09a92bc3b7fb0d597f901e953": {
					RequestType: "metrics_find", Id: "fd3e9fd09a92bc3b7fb0d597f901e953",
					TimeStamp:     1674288380528000000,
					Queries:       []Query{{Query: "test.c*"}},
					Metrics:       6,
					RequestStatus: 200, RequestTime: 0.174497662, QueryTime: 0.174497662,
					WaitStatus: 1,
					ReadRows:   413049, ReadBytes: 24262486,
					IndexReadRows: 413049, IndexReadBytes: 24262486,
					Index: []IndexStat{
						{
							Time: 0.174105795, Status: 1,
							ReadRows: 413049, ReadBytes: 24262486,
							Table:   "graphite_indexd",
							QueryId: "fd3e9fd09a92bc3b7fb0d597f901e953::9c3fc3cb99436f1b",
						},
					},
				},
			},
		},
		{
			name: "/metrics/find test.c* (cached)",
			entries: []string{
				`{"level":"INFO","timestamp":"2023-01-21T13:06:25.761+0500","logger":"metrics-find","message":"finder","request_id":"c9ec01a8b31079bfdfbc530a845f279c","get_cache":"1970-02-12;query=test.c*;ts=1674288000","metrics":6,"find_cached":true,"ttl":600,"query":["test.c*"]}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:06:25.761+0500","logger":"http","message":"access","request_id":"c9ec01a8b31079bfdfbc530a845f279c","time":0.00016375,"wait_slot":0,"wait_fail":false,"method":"GET","url":"/metrics/find/?format=carbonapi_v3_pb&query=test.c%2A","peer":"127.0.0.1:39818","client":"","status":200,"find_cached":true}`,
			},
			queries: map[string]*Stat{
				"c9ec01a8b31079bfdfbc530a845f279c": {
					RequestType: "metrics_find", Id: "c9ec01a8b31079bfdfbc530a845f279c",
					Queries:       []Query{{Query: "test.c*"}},
					TimeStamp:     1674288385761000000,
					RequestStatus: 200, RequestTime: 0.00016375, QueryTime: 0.00016375,
					WaitStatus: 1, Metrics: 6,
					Index: []IndexStat{{Status: StatusCached}},
				},
			},
		},
		{
			name: "/tags/autoComplete/tags?tagPrefix=c&expr='app=chproxy'",
			entries: []string{
				`{"level":"INFO","timestamp":"2023-01-21T13:07:04.355+0500","logger":"autocomplete","message":"query","request_id":"d4b7d5686f514502c362bafa608ca91b","query":"SELECT splitByChar('=', arrayJoin(Tags))[1] AS value FROM graphite_tagsd  WHERE ((Tag1='app=chproxy') AND (arrayJoin(Tags) LIKE 'c%')) AND (Date >= '2023-01-21' AND Date <= '2023-01-21') GROUP BY value ORDER BY value LIMIT 10001","written_rows":"0","written_bytes":"0","total_rows_to_read":"404694","read_rows":"404694","read_bytes":"160109507","query_id":"d4b7d5686f514502c362bafa608ca91b::4523f47e4368149d","time":0.175910111}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:07:04.355+0500","logger":"autocomplete","message":"tag_names","request_id":"d4b7d5686f514502c362bafa608ca91b","set_cache":"tags;2023-01-21;2023-01-21;limit=10000;tagPrefix=c;tag=;app=chproxy;ts=1674288000","metrics":5,"find_cached":false,"ttl":600,"query":["tagPrefix=c","expr='app=chproxy'","limit=10000"]}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:07:04.355+0500","logger":"http","message":"access","request_id":"d4b7d5686f514502c362bafa608ca91b","time":0.176225304,"wait_slot":0,"wait_fail":false,"method":"GET","url":"/tags/autoComplete/tags?format=json&tagPrefix=c&expr=app%3Dchproxy","peer":"127.0.0.1:52368","client":"","status":200,"find_cached":false}`,
			},
			queries: map[string]*Stat{
				"d4b7d5686f514502c362bafa608ca91b": {
					RequestType: "tag_names", Id: "d4b7d5686f514502c362bafa608ca91b",
					TimeStamp:     1674288424355000000,
					Queries:       []Query{{Query: "tagPrefix='c' expr='app=chproxy'"}},
					RequestStatus: 200, RequestTime: 0.176225304, QueryTime: 0.176225304,
					WaitStatus: 1,
					ReadRows:   404694, ReadBytes: 160109507,
					IndexReadRows: 404694, IndexReadBytes: 160109507,
					Index: []IndexStat{
						{
							Time: 0.175910111, Status: 1,
							ReadRows: 404694, ReadBytes: 160109507,
							Table:   "graphite_tagsd",
							QueryId: "d4b7d5686f514502c362bafa608ca91b::4523f47e4368149d",
						},
					},
				},
			},
		},
		{
			name: "/tags/autoComplete/tags?tagPrefix=c&expr='app=chproxy' (cached)",
			entries: []string{
				`{"level":"INFO","timestamp":"2023-01-21T13:07:11.664+0500","logger":"autocomplete","message":"finder","request_id":"b01d7c166c417300bb0b93863f27d47a","get_cache":"tags;2023-01-21;2023-01-21;limit=10000;tagPrefix=c;tag=;app=chproxy;ts=1674288000","metrics":5,"find_cached":true,"ttl":600,"query":["tagPrefix=c","expr='app=chproxy'","limit=10000"]}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:07:11.665+0500","logger":"http","message":"access","request_id":"b01d7c166c417300bb0b93863f27d47a","time":0.000198872,"wait_slot":0,"wait_fail":false,"method":"GET","url":"/tags/autoComplete/tags?format=json&tagPrefix=c&expr=app%3Dchproxy","peer":"127.0.0.1:45856","client":"","status":200,"find_cached":true}`,
			},
			queries: map[string]*Stat{
				"b01d7c166c417300bb0b93863f27d47a": {
					RequestType: "tag_names", Id: "b01d7c166c417300bb0b93863f27d47a",
					TimeStamp:     1674288431665000000,
					Queries:       []Query{{Query: "tagPrefix='c' expr='app=chproxy'"}},
					RequestStatus: 200, RequestTime: 0.000198872, QueryTime: 0.000198872,
					WaitStatus: 1, Metrics: 5,
					Index: []IndexStat{{Status: StatusCached}},
				},
			},
		},
		{
			name: "/tags/autoComplete/values?tag=c&name&expr='app=chproxy'",
			entries: []string{
				`{"level":"INFO","timestamp":"2023-01-21T17:59:29.232+0500","logger":"autocomplete","message":"query","request_id":"d7f506acefdc194c10a30cebabdfae06","query":"SELECT substr(arrayJoin(Tags), 10) AS value FROM graphite_tagsd  WHERE ((Tag1='app=chproxy') AND (arrayJoin(Tags) LIKE '\\\\_\\\\_name\\\\_\\\\_=%')) AND (Date >= '2023-01-21' AND Date <= '2023-01-21') GROUP BY value ORDER BY value LIMIT 10000","read_rows":"362995","read_bytes":"139629325","written_rows":"0","written_bytes":"0","total_rows_to_read":"362995","query_id":"d7f506acefdc194c10a30cebabdfae06::da2dc89e6ac4b9ae","time":0.147248531}`,
				`{"level":"INFO","timestamp":"2023-01-21T17:59:29.233+0500","logger":"autocomplete","message":"finder","request_id":"d7f506acefdc194c10a30cebabdfae06","set_cache":"values;2023-01-21;2023-01-21;limit=10000;valuePrefix=;tag=__name__;app=chproxy;ts=1674305400","metrics":71,"find_cached":false,"ttl":600,"query":["tag=__name__","expr='app=chproxy'","limit=10000"]}`,
				`{"level":"INFO","timestamp":"2023-01-21T17:59:29.233+0500","logger":"http","message":"access","request_id":"d7f506acefdc194c10a30cebabdfae06","time":0.147378304,"wait_slot":0,"wait_fail":false,"method":"GET","url":"/tags/autoComplete/values?format=json&tag=c&expr=app%3Dchproxy","peer":"127.0.0.1:38800","client":"","status":200,"find_cached":false}`,
			},
			queries: map[string]*Stat{
				"d7f506acefdc194c10a30cebabdfae06": {
					RequestType: "tag_values", Id: "d7f506acefdc194c10a30cebabdfae06",
					TimeStamp:     1674305969233000000,
					Queries:       []Query{{Query: "tag='c' expr='app=chproxy'"}},
					RequestStatus: 200, RequestTime: 0.147378304, QueryTime: 0.147378304,
					WaitStatus: 1, Metrics: 71,
					ReadRows: 362995, ReadBytes: 139629325,
					IndexReadRows: 362995, IndexReadBytes: 139629325,
					Index: []IndexStat{
						{
							Time: 0.147248531, Status: 1,
							ReadRows: 362995, ReadBytes: 139629325,
							Table:   "graphite_tagsd",
							QueryId: "d7f506acefdc194c10a30cebabdfae06::da2dc89e6ac4b9ae",
						},
					},
				},
			},
		},
		{
			name: "/tags/autoComplete/values?c&tag=c&expr='app=chproxy' (cached)",
			entries: []string{
				`{"level":"INFO","timestamp":"2023-01-21T13:10:49.693+0500","logger":"autocomplete","message":"finder","request_id":"755043946ebafc11639efa26a8fdc51d","get_cache":"values;2023-01-21;2023-01-21;limit=10000;valuePrefix=;tag=__name__;app=chproxy;ts=1674288600","metrics":71,"find_cached":true,"ttl":600,"query":["tag=__name__","expr='app=chproxy'","limit=10000"]}`,
				`{"level":"INFO","timestamp":"2023-01-21T13:10:49.693+0500","logger":"http","message":"access","request_id":"755043946ebafc11639efa26a8fdc51d","time":0.00038786,"wait_slot":0,"wait_fail":false,"method":"GET","url":"/tags/autoComplete/values?format=json&tag=c&expr=app+%3D+chproxy","peer":"127.0.0.1:38152","client":"","status":200,"find_cached":true}`,
			},
			queries: map[string]*Stat{
				"755043946ebafc11639efa26a8fdc51d": {
					RequestType: "tag_values", Id: "755043946ebafc11639efa26a8fdc51d",
					TimeStamp:     1674288649693000000,
					Queries:       []Query{{Query: "tag='c' expr='app=chproxy'"}},
					Metrics:       71,
					RequestStatus: 200, RequestTime: 0.00038786, QueryTime: 0.00038786,
					WaitStatus: 1,
					Index:      []IndexStat{{Status: StatusCached}},
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

			if !reflect.DeepEqual(tt.queries, queries) {
				t.Fatalf("LogEntryProcess() = %s", cmp.Diff(tt.queries, queries))
			}
		})
	}
}
