package utils

import (
	"fmt"
	"strconv"
	"testing"
)

func TestPercentile(t *testing.T) {
	tests := []struct {
		name    string
		input   []float64
		percent float64
		want    float64
		wantErr bool
	}{
		{
			name:    "Median []",
			input:   []float64{},
			percent: 0.5,
			wantErr: true,
		},
		{
			name:    "Median []",
			input:   []float64{0.1},
			percent: 0.5,
			want:    0.1,
		},
		{
			name:    "Median []",
			input:   []float64{0.1, 0.3},
			percent: 0.5,
			want:    0.1,
		},
		{
			name:    "Median []",
			input:   []float64{0.1, 0.3},
			percent: 0.9,
			want:    0.2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Percentile(tt.input, tt.percent)
			if (err != nil) != tt.wantErr {
				t.Errorf("Percentile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Percentile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFormatSeconds(t *testing.T) {
	tests := []struct {
		sec  int64
		want string
	}{
		{0, ""},
		{1, "1s"},
		{10, "10s"},
		{15, "10s"}, // down to 10s
		{16, "20s"}, // up to 20s
		{50, "50s"},
		{54, "50s"}, // down to 50s
		{55, "1m"},  // up to 1m
		{60, "1m"},
		{90, "1m"}, // down to 1m
		{91, "2m"}, // up to 2m
		{120, "2m"},
		{150, "2m"}, // down to 2m
		{151, "3m"}, // up to 3m
		{180, "3m"},
		{1800, "30m"}, // down to 30m
		{1801, "1h"},  // up to 1h
		{3600, "1h"},
		{5400, "1h"}, // down to 1h
		{5401, "2h"}, // up to 2h
		{7200, "2h"},
		{10800, "3h"},
		{79200, "22h"},
		{81000, "22h"}, // down to 22h
		{81001, "23h"}, // up to 23h
		{82800, "23h"},
		{84600, "23h"}, // down to 23h
		{84601, "1d"},  // up to 1d
		{86400, "1d"},
		{129600, "1d"}, // down to 1d
		{129601, "2d"}, // up to 2d
		{172800, "2d"},
		{253800, "3d"},
		{2548800, "29d"},
		{2548801, "1M"}, // up to 1M
		{2592000, "1M"},
		{2 * 2592000, "2M"},
		{28944000, "11M"},
		{28944001, "1Y"}, // up to 1Y
		{31536000, "1Y"},
		{2*31536000 - 15768000, "1Y"},     // down to 1Y
		{2*31536000 - 15768000 + 1, "2Y"}, // up to 2Y
		{2 * 31536000, "2Y"},
	}
	for _, tt := range tests {
		t.Run(strconv.FormatInt(tt.sec, 10), func(t *testing.T) {
			if got := FormatTruncSeconds(tt.sec); got != tt.want {
				t.Errorf("FormatSeconds() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		sec    int64
		offset bool
		want   string
	}{
		{0, false, ""},
		{1, false, "10m"},
		{600, false, "10m"}, // up to 2m
		{601, false, "1h"},
		{3600, false, "1h"},
		{3601, false, "6h"},
		{21600, false, "6h"}, // 3600 * 6
		{21601, false, "12h"},
		{43200, false, "12h"}, // 3600 * 12
		{43201, false, "1d"},
		{86399, true, ""},    // 3600 * 24 -1
		{86400, false, "1d"}, // 3600 * 24
		{86400, true, "1d"},  // 3600 * 24
		{86401, false, "2d"},
		{172800, false, "2d"}, // 3600 * 24 * 2
		{172801, false, "7d"},
		{604800, false, "7d"}, // 3600 * 24 * 7
		{604801, false, "1M"},
		{2592000, false, "1M"}, // 3600 * 24 * 30
		{2592001, false, "3M"},
		{7776000, false, "3M"}, // 3600 * 24 * 90
		{7776001, false, "6M"},
		{15552000, false, "6M"}, // 3600 * 24 * 180
		{15552001, false, "1Y"},
		{31536000, false, "1Y"},                  // 3600 * 24 * 365
		{2*31536000 - 15768000, false, "1Y"},     // down to 1Y
		{2*31536000 - 15768000 + 1, false, "2Y"}, // up to 2Y
		{2 * 31536000, false, "2Y"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d %v", tt.sec, tt.offset), func(t *testing.T) {
			if got := FormatDuration(tt.sec, tt.offset); got != tt.want {
				t.Errorf("FormatSeconds() = %v, want %v", got, tt.want)
			}
		})
	}
}
