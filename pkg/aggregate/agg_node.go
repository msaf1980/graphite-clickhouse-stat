package aggregate

import (
	"sort"

	"github.com/msaf1980/graphite-clickhouse-stat/pkg/utils"
)

type AggNode struct {
	Min float64
	Max float64
	P50 float64
	P90 float64
	P95 float64
	P99 float64
}

func (a *AggNode) Calc(values []float64) error {
	if len(values) == 0 {
		return utils.ErrEmptyInput
	}

	input := make([]float64, len(values))
	copy(input, values)
	sort.Float64s(input)

	var err error

	a.Min = input[0]
	a.Max = input[len(input)-1]
	// a.Sum = utils.Sum(input)

	if a.P50, err = utils.Percentile(input, 0.5); err != nil {
		return err
	}
	if a.P90, err = utils.Percentile(input, 0.9); err != nil {
		return err
	}
	if a.P95, err = utils.Percentile(input, 0.95); err != nil {
		return err
	}
	if a.P99, err = utils.Percentile(input, 0.99); err != nil {
		return err
	}

	return nil
}
