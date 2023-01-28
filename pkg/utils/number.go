package utils

import (
	"errors"
	"math"
	"strconv"
)

var (
	ErrEmptyInput = errors.New("input must not be empty")
	ErrBounds     = errors.New("input is outside of range")
)

func roundN(val, n int64) (v int64) {
	v = val / n * n
	diff := val - v
	if diff > n/2 {
		v += n
	}
	return
}

func FormatDuration(n int64, offset bool) string {
	if n == 0 || (offset && n < 3600*24) {
		return ""
	}

	var sign string
	if n < 0 {
		sign = "-1"
		n = -n
	}
	if n <= 600 {
		return sign + "10m"
	}
	if n <= 3600 {
		return sign + "1h"
	}
	if n <= 3600*6 {
		return sign + "6h"
	}
	if n <= 3600*12 {
		return sign + "12h"
	}
	if n <= 3600*24 {
		return sign + "1d"
	}
	if n <= 3600*24*2 {
		return sign + "2d"
	}
	if n <= 3600*24*7 {
		return sign + "7d"
	}
	if n <= 3600*24*30 {
		return sign + "1M"
	}
	if n <= 3600*24*90 {
		return sign + "3M"
	}
	if n <= 3600*24*90*2 {
		return sign + "6M"
	}
	if n <= 3600*24*365 {
		return sign + "1Y"
	}
	v := roundN(n, 31536000) / 31536000
	return sign + strconv.FormatInt(v, 10) + "Y"
}

func FormatTruncSeconds(n int64) string {
	if n == 0 {
		return ""
	}
	sign := int64(1)
	if n < 0 {
		sign = -1
		n = -n
	}
	if n > 31536000-2592000 {
		v := roundN(n, 31536000) / 31536000
		return strconv.FormatInt(sign*v, 10) + "Y"
	}
	if n > 2592000-43200 {
		v := roundN(n, 2592000) / 2592000
		return strconv.FormatInt(sign*v, 10) + "M"
	}
	if n > 86400-1800 {
		v := roundN(n, 86400) / 86400
		return strconv.FormatInt(sign*v, 10) + "d"
	}
	if n > 1800 {
		v := roundN(n, 3600) / 3600
		return strconv.FormatInt(sign*v, 10) + "h"
	}
	if n >= 55 {
		v := roundN(n, 60) / 60
		return strconv.FormatInt(sign*v, 10) + "m"
	}
	if n > 10 {
		v := roundN(n, 10)
		return strconv.FormatInt(sign*v, 10) + "s"
	}
	return strconv.FormatInt(sign*n, 10) + "s"
}

func FormatBytes(n int64) string {
	if n == 0 {
		return ""
	}
	if n > 20*1073741824 {
		return strconv.FormatInt(n/(1073741824), 10) + "G"
	}
	if n > 20*1048576 {
		return strconv.FormatInt(n/(1048576), 10) + "M"
	}
	if n > 20*1024 {
		return strconv.FormatInt(n/(1024), 10) + "K"
	}
	return strconv.FormatInt(n, 10)
}

func FormatNumber(n int64) string {
	if n == 0 {
		return ""
	}
	if n > 20*1e9 {
		return strconv.FormatInt(n/(1e9), 10) + "g"
	}
	if n > 20*1e6 {
		return strconv.FormatInt(n/(1e6), 10) + "m"
	}
	if n > 20*1000 {
		return strconv.FormatInt(n/(1000), 10) + "k"
	}
	return strconv.FormatInt(n, 10)
}

func FormatInt(n int) string {
	if n == 0 {
		return ""
	}
	return strconv.Itoa(n)
}

func FormatInt64(n int64) string {
	if n == 0 {
		return ""
	}
	return strconv.FormatInt(n, 10)
}

func FormatFloat64(n float64, prec int) string {
	if n == 0.0 {
		return ""
	}
	if n > 20.0*1e9 {
		return strconv.FormatFloat(n/(1e9), 'f', prec, 32) + "g"
	}
	if n > 20*1e6 {
		return strconv.FormatFloat(n/(1e6), 'f', prec, 32) + "m"
	}
	if n > 20*1000 {
		return strconv.FormatFloat(n/(1000), 'f', prec, 32) + "k"
	}
	return strconv.FormatFloat(n, 'f', prec, 32)
}

func FormatFloat64Z(n float64, prec int) string {
	if n == 0.0 {
		return "0"
	}
	if n > 20.0*1e9 {
		return strconv.FormatFloat(n/(1e9), 'f', prec, 32) + "g"
	}
	if n > 20*1e6 {
		return strconv.FormatFloat(n/(1e6), 'f', prec, 32) + "m"
	}
	if n > 20*1000 {
		return strconv.FormatFloat(n/(1000), 'f', prec, 32) + "k"
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

func FormatPcnt(n float64) string {
	if n == 0.0 {
		return ""
	}
	if n >= 1.0 {
		return strconv.FormatFloat(n, 'f', 2, 32)
	}

	return strconv.FormatFloat(n, 'f', 4, 32)
}
