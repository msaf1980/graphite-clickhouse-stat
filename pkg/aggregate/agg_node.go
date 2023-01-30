package aggregate

import (
	"sort"

	"github.com/msaf1980/graphite-clickhouse-stat/pkg/utils"
)

type AggNode struct {
	Init bool
	Min  float64
	Max  float64
	P50  float64
	P90  float64
	P95  float64
	P99  float64
}

func (agg *AggNode) Calc(values []float64) error {
	if len(values) == 0 {
		agg.Init = false
		return utils.ErrEmptyInput
	}

	input := make([]float64, len(values))
	copy(input, values)
	sort.Float64s(input)

	var err error

	agg.Min = input[0]
	agg.Max = input[len(input)-1]
	// a.Sum = utils.Sum(input)

	if agg.P50, err = utils.Percentile(input, 0.5); err != nil {
		agg.Init = false
		return err
	}
	if agg.P90, err = utils.Percentile(input, 0.9); err != nil {
		agg.Init = false
		return err
	}
	if agg.P95, err = utils.Percentile(input, 0.95); err != nil {
		agg.Init = false
		return err
	}
	if agg.P99, err = utils.Percentile(input, 0.99); err != nil {
		agg.Init = false
		return err
	}

	agg.Init = true

	return nil
}

func (agg *AggNode) Diff(a, b *AggNode) {
	if a.Init && b.Init {

	} else {
		agg.Init = false
		agg.Min = 0
		agg.Max = 0
	}
}
