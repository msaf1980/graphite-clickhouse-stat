package utils

import (
	"fmt"
	"strconv"
)

type Int64Slice []int64

func (u *Int64Slice) Set(value string) error {
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return err
	}
	*u = append(*u, n)
	return nil
}

func (u *Int64Slice) String() string {
	return fmt.Sprintf("%v", *u)
}

func (u *Int64Slice) Type() string {
	return "[]int64"
}
