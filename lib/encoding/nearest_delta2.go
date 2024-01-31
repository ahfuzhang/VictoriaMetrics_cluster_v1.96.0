package encoding

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// marshalInt64NearestDelta2 encodes src using `nearest delta2` encoding
// with the given precisionBits and appends the encoded value to dst.
//
// precisionBits must be in the range [1...64], where 1 means 50% precision,
// while 64 means 100% precision, i.e. lossless encoding.
func marshalInt64NearestDelta2(dst []byte, src []int64, precisionBits uint8) (result []byte, firstValue int64) {  // 序列化 values, timestamp 的时候调用，至少消耗 4%
	if len(src) < 2 {
		logger.Panicf("BUG: src must contain at least 2 items; got %d items", len(src))
	}
	if err := CheckPrecisionBits(precisionBits); err != nil {
		logger.Panicf("BUG: %s", err)
	}

	firstValue = src[0]
	d1 := src[1] - src[0]
	dst = MarshalVarInt64(dst, d1)
	v := src[1]
	src = src[2:]
	is := GetInt64s(len(src))  // pool 中获得一个目的数组
	if precisionBits == 64 {
		// Fast path.
		for i, next := range src {  // 通常执行到这里
			d2 := next - v - d1
			d1 += d2
			v += d1
			is.A[i] = d2  // todo: 这种位置很适合 simd
		}
		// tailLoc := len(src) - len(src)&7
		// topHalf := src[:tailLoc]
		// for i:=0; i<len(topHalf); i+=8{
		// 	next := topHalf[i]
		// 	d2 := next - v - d1
		// 	d1 += d2
		// 	v += d1
		// 	is.A[i] = d2
		// }
		// bottomHalf := src[tailLoc:]
	} else {
		// Slower path.
		trailingZeros := getTrailingZeros(v, precisionBits)
		for i, next := range src {
			d2, tzs := nearestDelta(next-v, d1, precisionBits, trailingZeros)
			trailingZeros = tzs
			d1 += d2
			v += d1
			is.A[i] = d2
		}
	}
	dst = MarshalVarInt64s(dst, is.A)
	//dst = MarshalVarInt64sBySearchTable(dst, is.A)  // 这个东西不是用来编码字符串长度的。就是为了编码 int 本身。所以查表没有用
	PutInt64s(is)
	return dst, firstValue
}

// unmarshalInt64NearestDelta2 decodes src using `nearest delta2` encoding,
// appends the result to dst and returns the appended result.
//
// firstValue must be the value returned from marshalInt64NearestDelta2.
func unmarshalInt64NearestDelta2(dst []int64, src []byte, firstValue int64, itemsCount int) ([]int64, error) {
	if itemsCount < 2 {
		logger.Panicf("BUG: itemsCount must be greater than 1; got %d", itemsCount)
	}

	is := GetInt64s(itemsCount - 1)
	defer PutInt64s(is)

	tail, err := UnmarshalVarInt64s(is.A, src)  // 解码整个数组
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal nearest delta from %d bytes; src=%X: %w", len(src), src, err)
	}
	if len(tail) > 0 {
		return nil, fmt.Errorf("unexpected tail left after unmarshaling %d items from %d bytes; tail size=%d; src=%X; tail=%X", itemsCount, len(src), len(tail), src, tail)
	}

	v := firstValue
	d1 := is.A[0]
	dst = append(dst, v)
	v += d1
	dst = append(dst, v)
	for _, d2 := range is.A[1:] {
		d1 += d2
		v += d1
		dst = append(dst, v)
	}
	return dst, nil
}
