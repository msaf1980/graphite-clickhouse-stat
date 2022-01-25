package utils

import (
	"errors"
	"math"
	"strconv"
)

var (
	ErrEmptyInput = errors.New("Input must not be empty")
	ErrBounds     = errors.New("Input is outside of range")
)

func FormatNumber(n int64) string {
	if n > 20*1024*1024*1024 {
		return strconv.FormatInt(n/(1024*1024*1024), 10) + "G"
	}
	if n > 20*1024*1024 {
		return strconv.FormatInt(n/(1024*1024), 10) + "M"
	}
	if n > 20*1024 {
		return strconv.FormatInt(n/(1024), 10) + "k"
	}
	return strconv.FormatInt(n, 10)
}

func FormatFloat64(n float64, prec int) string {
	if n > 20.0*1024*1024*1024 {
		return strconv.FormatFloat(n/(1024*1024*1024), 'f', prec, 32) + "G"
	}
	if n > 20*1024*1024 {
		return strconv.FormatFloat(n/(1024*1024), 'f', prec, 32) + "M"
	}
	if n > 20*1024 {
		return strconv.FormatFloat(n/(1024), 'f', prec, 32) + "k"
	}
	return strconv.FormatFloat(n, 'f', prec, 32)
}

func Sum(input []float64) float64 {
	var sum float64

	for _, v := range input {
		sum += v
	}

	return sum
}

// Percentile Calc percentile on sorted slice
func Percentile(input []float64, percent float64) (percentile float64, err error) {
	length := len(input)
	if length == 0 {
		return math.NaN(), ErrEmptyInput
	}

	if length == 1 {
		return input[0], nil
	}

	if percent <= 0 || percent > 1.0 {
		return math.NaN(), ErrBounds
	}

	// Multiply percent by length of input
	index := percent * float64(len(input))

	// Check if the index is a whole number
	if index == float64(int64(index)) {
		// Convert float to int
		i := int(index)

		// Find the value at the index
		percentile = input[i-1]
	} else if index > 1 {
		// Convert float to int via truncation
		i := int(index)

		// Find the average of the index and following values
		percentile = (input[i-1] + input[i]) / 2
	} else {
		return math.NaN(), ErrBounds
	}

	return percentile, nil
}
