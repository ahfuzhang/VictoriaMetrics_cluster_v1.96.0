package encoding

func IsConst(a []int64) bool

func MyTestIsConst(a []int64) bool {
	return len(a) > 0
}

// go build -gcflags="-S" is_const.go
// go tool compile -S is_const.go
// arm64 中到底是怎么返回值的，仍然不清楚


