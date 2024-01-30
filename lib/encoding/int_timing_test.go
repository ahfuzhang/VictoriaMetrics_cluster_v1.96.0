package encoding

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func BenchmarkMarshalUint64(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		var dst []byte
		var sink uint64
		for pb.Next() {
			dst = MarshalUint64(dst[:0], sink)
			sink += uint64(len(dst))
		}
		atomic.AddUint64(&Sink, sink)
	})
}

func BenchmarkUnmarshalUint64(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		var sink uint64
		for pb.Next() {
			v := UnmarshalUint64(testMarshaledUint64Data)
			sink += v
		}
		atomic.AddUint64(&Sink, sink)
	})
}

func BenchmarkMarshalInt64(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		var dst []byte
		var sink uint64
		for pb.Next() {
			dst = MarshalInt64(dst[:0], int64(sink))
			sink += uint64(len(dst))
		}
		atomic.AddUint64(&Sink, sink)
	})
}

func BenchmarkUnmarshalInt64(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		var sink uint64
		for pb.Next() {
			v := UnmarshalInt64(testMarshaledInt64Data)
			sink += uint64(v)
		}
		atomic.AddUint64(&Sink, sink)
	})
}

/*
BenchmarkMarshalVarInt64s/up-to-(1<<6)-1-10              1724980               775.6 ns/op      10314.17 MB/s          0 B/op          0 allocs/op
=== RUN   BenchmarkMarshalVarInt64s/up-to-(1<<13)-1
BenchmarkMarshalVarInt64s/up-to-(1<<13)-1
BenchmarkMarshalVarInt64s/up-to-(1<<13)-1-10              559910              1849 ns/op        4327.42 MB/s           1 B/op          0 allocs/op
=== RUN   BenchmarkMarshalVarInt64s/up-to-(1<<27)-1
BenchmarkMarshalVarInt64s/up-to-(1<<27)-1
BenchmarkMarshalVarInt64s/up-to-(1<<27)-1-10              342729              3594 ns/op        2225.66 MB/s           4 B/op          0 allocs/op
=== RUN   BenchmarkMarshalVarInt64s/up-to-(1<<63)-1
BenchmarkMarshalVarInt64s/up-to-(1<<63)-1
BenchmarkMarshalVarInt64s/up-to-(1<<63)-1-10              220304              5447 ns/op        1468.63 MB/s          17 B/op          0 allocs/op
*/
func BenchmarkMarshalVarInt64s(b *testing.B) {
	b.Run("up-to-(1<<6)-1", func(b *testing.B) {
		benchmarkMarshalVarInt64s(b, (1<<6)-1)
	})
	b.Run("up-to-(1<<13)-1", func(b *testing.B) {
		benchmarkMarshalVarInt64s(b, (1<<13)-1)
	})
	b.Run("up-to-(1<<27)-1", func(b *testing.B) {
		benchmarkMarshalVarInt64s(b, (1<<27)-1)
	})
	b.Run("up-to-(1<<63)-1", func(b *testing.B) {
		benchmarkMarshalVarInt64s(b, (1<<63)-1)
	})
}

func benchmarkMarshalVarInt64s(b *testing.B, maxValue int64) {
	const numsCount = 8000
	var data []int64
	n := maxValue
	for i := 0; i < numsCount; i++ {
		if n <= 0 {
			n = maxValue
		}
		data = append(data, n)
		n--
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(numsCount)
	b.RunParallel(func(pb *testing.PB) {
		var sink uint64
		var dst []byte
		for pb.Next() {
			//dst = MarshalVarInt64s(dst[:0], data)
			//dst = MarshalVarInt64sV9(dst[:0], data)
			dst = MarshalVarInt64sV11(dst[:0], data)
			sink += uint64(len(dst))
		}
		atomic.AddUint64(&Sink, sink)
	})
}

func BenchmarkUnmarshalVarInt64s(b *testing.B) {
	b.Run("up-to-(1<<6)-1", func(b *testing.B) {
		benchmarkUnmarshalVarInt64s(b, (1<<6)-1)
	})
	b.Run("up-to-(1<<13)-1", func(b *testing.B) {
		benchmarkUnmarshalVarInt64s(b, (1<<13)-1)
	})
	b.Run("up-to-(1<<27)-1", func(b *testing.B) {
		benchmarkUnmarshalVarInt64s(b, (1<<27)-1)
	})
	b.Run("up-to-(1<<63)-1", func(b *testing.B) {
		benchmarkUnmarshalVarInt64s(b, (1<<63)-1)
	})
}

func benchmarkUnmarshalVarInt64s(b *testing.B, maxValue int64) {
	const numsCount = 8000
	var data []byte
	n := maxValue
	for i := 0; i < numsCount; i++ {
		if n <= 0 {
			n = maxValue
		}
		data = MarshalVarInt64(data, n)
		n--
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(numsCount)
	b.RunParallel(func(pb *testing.PB) {
		var sink uint64
		dst := make([]int64, numsCount)
		for pb.Next() {
			tail, err := UnmarshalVarInt64s(dst, data)
			if err != nil {
				panic(fmt.Errorf("unexpected error: %w", err))
			}
			if len(tail) > 0 {
				panic(fmt.Errorf("unexpected non-empty tail with len=%d: %X", len(tail), tail))
			}
			sink += uint64(len(dst))
		}
		atomic.AddUint64(&Sink, sink)
	})
}

var testMarshaledInt64Data = MarshalInt64(nil, 1234567890)
var testMarshaledUint64Data = MarshalUint64(nil, 1234567890)

// 4455 ns/op
// 6594 ns/op           41016 B/op          4 allocs/op
// 4682 ns/op              56 B/op          3 allocs/op
// go test -benchmem -run=^$ -bench ^Benchmark_MarshalVarInt64s_v11$ github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding
func Benchmark_MarshalVarInt64s_v11(b *testing.B) {
	datas := []uint64{0, UintRange7Bit - 1, UintRange7Bit,
		UintRange14Bit - 1, UintRange14Bit,
		UintRange21Bit - 1, UintRange21Bit,
		UintRange28Bit - 1, UintRange28Bit,
		UintRange35Bit - 1, UintRange35Bit,
		UintRange42Bit - 1, UintRange42Bit,
		UintRange49Bit - 1, UintRange49Bit,
		UintRange56Bit - 1, UintRange56Bit,
		UintRange63Bit - 1, UintRange63Bit,
		UintRange14Bit + 1, 0xFFFFFFFFFFFFFFFF,
	}
	arr := make([]int64, 0, len(datas)*100)
	for i := 0; i < 100; i++ {
		for _, v := range datas {
			arr = append(arr, ZigzagDecode(v))
		}
	}
	var buf [1024 * 32]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MarshalVarInt64sV12(buf[:0], arr)
	}
}

/*
38708 ns/op	       0 B/op	       0 allocs/op  linux, switch-case 版
32930 ns/op	       0 B/op	       0 allocs/op   linux, fastcgo 版本

env CC=clang CGO_ENABLED=1  GOOS=linux  GOARCH=amd64 CGO_CFLAGS="-O2" CGO_LDFLAGS="-static" \
go test -ldflags="-w -s" -gcflags="-B" -benchmem -run=^$ -bench ^Benchmark_MarshalVarInt64s_v11$ github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding

36505 ns/op	       0 B/op	       0 allocs/op  加了优化参数反而慢了

=========================
arm64
4629 ns/op              56 B/op          3 allocs/op  cgo 版本
4491 ns/op               0 B/op          0 allocs/op  switch-case 版本

env CC=clang CGO_ENABLED=1 CGO_CFLAGS="-O2"  \
go test -ldflags="-w -s" -gcflags="-B" -benchmem -run=^$ -bench ^Benchmark_MarshalVarInt64s_v11$ github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding

4682 ns/op              56 B/op          3 allocs/op  cgo，加了 -O2 的版本
*/
