package helpers

import (
	"strconv"
	"strings"
)

func StringWhiteSpace(s string) string {
	return strings.Trim(s, " ")
}

func ConvertStrToFloat(s string) float64 {
	f, _ := strconv.ParseFloat(StringWhiteSpace(s), 64)
	return f
}
