package logstorage

import (
	"fmt"
	"testing"
	"unsafe"
)

func BenchmarkBlock_MustInitFromRows(b *testing.B) {
	for _, rowsPerBlock := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("rowsPerBlock_%d", rowsPerBlock), func(b *testing.B) {
			benchmarkBlockMustInitFromRows(b, rowsPerBlock)
		})
	}
}

func benchmarkBlockMustInitFromRows(b *testing.B, rowsPerBlock int) {
	timestamps, rows := newTestRows(rowsPerBlock, 10)
	b.ReportAllocs()
	b.SetBytes(int64(len(timestamps)))
	b.RunParallel(func(pb *testing.PB) {
		block := getBlock()
		defer putBlock(block)
		for pb.Next() {
			block.MustInitFromRows(timestamps, rows)
			if n := block.Len(); n != len(timestamps) {
				panic(fmt.Errorf("unexpected block length; got %d; want %d", n, len(timestamps)))
			}
		}
	})
}

func newTestRows(rowsCount, fieldsPerRow int) ([]int64, [][]Field) {
	timestamps := make([]int64, rowsCount)
	rows := make([][]Field, rowsCount)
	for i := range timestamps {
		timestamps[i] = int64(i) * 1e9
		fields := make([]Field, fieldsPerRow)
		for j := range fields {
			f := &fields[j]
			f.Name = fmt.Sprintf("field_%d", j)
			f.Value = fmt.Sprintf("value_%d_%d", i, j)
		}
		rows[i] = fields
	}
	return timestamps, rows
}

func areTimestampsSorted(ts []int64) bool {
	if len(ts) < 2 {
		return true
	}
	for i := 1; i < len(ts); i++ {
		if ts[i] < ts[i-1] {
			return false
		}
	}
	return true
}

func getDeltaData(l int) []int64 {
	arr := make([]int64, l)
	for i := 0; i < l; i++ {
		arr[i] = int64(i + 1)
	}
	return arr
}

// 29058.59 MB/s
// 34774.06 MB/s  优化后版本， 提升 16.4
// 42793.81 MB/s,  提升 32%
func Benchmark_areTimestampsSorted(b *testing.B) {
	arr := getDeltaData(1024 * 1024)
	b.SetBytes(int64(len(arr)) * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !areTimestampsSorted(arr) {
			b.Error("should be true")
			return
		}
	}
}

func areTimestampsSortedV2(ts []int64) bool {
	if len(ts) < 2 {
		return true
	}
	l := len(ts) - 1
	align4 := l & (-4)
	tailLen := l & 3
	ptr := unsafe.Pointer(unsafe.SliceData(ts))
	for i := 1; i < align4+1; i += 4 {
		arr := (*[5]int64)(unsafe.Add(ptr, (i-1)*8))
		sign := ((arr[1] - arr[0]) >> 63) |
			((arr[2] - arr[1]) >> 63) |
			((arr[3] - arr[1]) >> 63) |
			((arr[4] - arr[3]) >> 63)
		if sign != 0 {
			//panic("not sorted")
			return false
		}
	}
	ptr = unsafe.Add(ptr, align4*8)
	var sign int64
	switch tailLen {
	case 1:
		arr := (*[2]int64)(ptr)
		sign = ((arr[1] - arr[0]) >> 63)
	case 2:
		arr := (*[3]int64)(ptr)
		sign = ((arr[1] - arr[0]) >> 63) | ((arr[2] - arr[1]) >> 63)
	case 3:
		arr := (*[4]int64)(ptr)
		sign = ((arr[1] - arr[0]) >> 63) | ((arr[2] - arr[1]) >> 63) | ((arr[3] - arr[2]) >> 63)
	default:
		panic("error")
	}
	// if sign != 0 {
	// 	panic("not")
	// }
	return sign == 0
}

func Benchmark_areTimestampsSortedV2(b *testing.B) {
	arr := getDeltaData(1024 * 1024)
	b.SetBytes(int64(len(arr)) * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !areTimestampsSortedV2(arr) {
			b.Error("should be true")
			return
		}
	}
}

func Test_v2(t *testing.T) {
	arr := getDeltaData(1024 * 1024)
	if !areTimestampsSortedV3(arr) {
		t.Error("should be true")
		return
	}
}

func areTimestampsSortedV3(ts []int64) bool {
	if len(ts) < 2 {
		return true
	}
	l := len(ts) - 1
	align4 := l & (-8)
	tailLen := l & 7
	ptr := unsafe.Pointer(unsafe.SliceData(ts))
	for i := 1; i < align4+1; i += 8 {
		arr := (*[9]int64)(unsafe.Add(ptr, (i-1)*8))
		sign := ((arr[1] - arr[0]) >> 63) |
			((arr[2] - arr[1]) >> 63) |
			((arr[3] - arr[1]) >> 63) |
			((arr[4] - arr[3]) >> 63) |
			((arr[5] - arr[4]) >> 63) |
			((arr[6] - arr[5]) >> 63) |
			((arr[7] - arr[6]) >> 63) |
			((arr[8] - arr[7]) >> 63)
		if sign != 0 {
			//panic("not sorted")
			return false
		}
	}
	ptr = unsafe.Add(ptr, align4*8)
	var sign int64
	switch tailLen {
	case 1:
		arr := (*[2]int64)(ptr)
		sign = ((arr[1] - arr[0]) >> 63)
	case 2:
		arr := (*[3]int64)(ptr)
		sign = ((arr[1] - arr[0]) >> 63) | ((arr[2] - arr[1]) >> 63)
	case 3:
		arr := (*[4]int64)(ptr)
		sign = ((arr[1] - arr[0]) >> 63) | ((arr[2] - arr[1]) >> 63) | ((arr[3] - arr[2]) >> 63)
	case 4:
		arr := (*[5]int64)(ptr)
		sign = ((arr[1] - arr[0]) >> 63) |
			((arr[2] - arr[1]) >> 63) |
			((arr[3] - arr[2]) >> 63) |
			((arr[4] - arr[3]) >> 63)
	case 5:
		arr := (*[6]int64)(ptr)
		sign = ((arr[1] - arr[0]) >> 63) |
			((arr[2] - arr[1]) >> 63) |
			((arr[3] - arr[2]) >> 63) |
			((arr[4] - arr[3]) >> 63) |
			((arr[5] - arr[4]) >> 63)
	case 6:
		arr := (*[7]int64)(ptr)
		sign = ((arr[1] - arr[0]) >> 63) |
			((arr[2] - arr[1]) >> 63) |
			((arr[3] - arr[2]) >> 63) |
			((arr[4] - arr[3]) >> 63) |
			((arr[5] - arr[4]) >> 63) |
			((arr[6] - arr[5]) >> 63)
	case 7:
		arr := (*[8]int64)(ptr)
		sign = ((arr[1] - arr[0]) >> 63) |
			((arr[2] - arr[1]) >> 63) |
			((arr[3] - arr[2]) >> 63) |
			((arr[4] - arr[3]) >> 63) |
			((arr[5] - arr[4]) >> 63) |
			((arr[6] - arr[5]) >> 63) |
			((arr[7] - arr[6]) >> 63)
	case 8:
		panic("impossible")
	default:
		panic("error")
	}
	// if sign != 0 {
	// 	panic("not")
	// }
	return sign == 0
}

// 42793.81 MB/s
func Benchmark_areTimestampsSortedV3(b *testing.B) {
	arr := getDeltaData(1024 * 1024)
	b.SetBytes(int64(len(arr)) * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !areTimestampsSortedV3(arr) {
			b.Error("should be true")
			return
		}
	}
}
