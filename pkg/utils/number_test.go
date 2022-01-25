package utils

import (
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
