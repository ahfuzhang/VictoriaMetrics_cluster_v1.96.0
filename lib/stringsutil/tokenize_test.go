package stringsutil

import (
	"math/rand"
	"os"
	"testing"
)

const longStr = "1-3-5-7-9-"

func TestToken_1(t *testing.T) {
	//Tokenize([]byte("123 456"), func(s string, isUnicode bool) {
	//	t.Log(s, isUnicode)
	//})
	//Tokenize([]byte(longStr+longStr+longStr+longStr+longStr+longStr+"123 456"), func(s string, isUnicode bool) {
	//	t.Log(s, isUnicode)
	//})
	s := longStr + longStr + longStr + longStr + longStr + longStr + longStr + longStr + "123 456"
	f := func(s string, isUnicode bool) {
		if !IsASCII(s) {
			panic("fail")
		}
		t.Log(s, isUnicode)
	}
	Tokenize([]byte(s[rand.Intn(7)+1:]), f)
	Tokenize(getString(7, '1'), f)
	Tokenize(getString(63, '1'), f)
	arr := getString(127, '1')
	Tokenize(arr, f)
	Tokenize(arr[3:], f)
	Tokenize(arr[3+8:], f)
	Tokenize(arr[3+16:], f)
	Tokenize(arr[3+24:], f)
	Tokenize(arr[3+32:], f)
	Tokenize(arr[3+40:], f)
	Tokenize(arr[3+48:], f)
	Tokenize(arr[3+36:], f)
	//
	Tokenize(arr[3+1:], f)
	Tokenize(arr[3+2:], f)
	Tokenize(arr[3+3:], f)
	Tokenize(arr[3+4:], f)
	Tokenize(arr[3+5:], f)
	Tokenize(arr[3+6:], f)
	Tokenize(arr[3+7:], f)
	//
	Tokenize(arr[:1], f)
	Tokenize(arr[:2], f)
	Tokenize(arr[:3], f)
	Tokenize(arr[:4], f)
	Tokenize(arr[:5], f)
	Tokenize(arr[:6], f)
	Tokenize(arr[:7], f)
	Tokenize(arr[:8], f)
	//
	arr2 := arr[1:]
	Tokenize(arr2[:1], f)
	Tokenize(arr2[:2], f)
	Tokenize(arr2[:3], f)
	Tokenize(arr2[:4], f)
	Tokenize(arr2[:5], f)
	Tokenize(arr2[:6], f)
	Tokenize(arr2[:7], f)
	Tokenize(arr2[:8], f)
	//
	Tokenize([]byte{}, nil)
	//
	arr = getString(1024, '-')
	Tokenize(arr, f)
	Tokenize(arr[1:], f)
	Tokenize(arr[2:], f)
	Tokenize(arr[3:], f)
	Tokenize(arr[4:], f)
	Tokenize(arr[5:], f)
	Tokenize(arr[6:], f)
	Tokenize(arr[7:], f)
	Tokenize(arr[8:], f)
	//
	empty := func(s string, isUnicode bool) {}
	arr = getString(1024, 0xff)
	Tokenize(arr, empty)
	Tokenize(arr[1:], empty)
	Tokenize(arr[2:], empty)
	Tokenize(arr[3:], empty)
	Tokenize(arr[4:], empty)
	Tokenize(arr[5:], empty)
	Tokenize(arr[6:], empty)
	Tokenize(arr[7:], empty)
	Tokenize(arr[8:], empty)
	Tokenize(arr[5:5+1], empty)
	Tokenize(arr[5:5+2], empty)
	Tokenize(arr[5:5+3], empty)
	Tokenize(arr[5:5+4], empty)
	Tokenize(arr[5:5+5], empty)
	Tokenize(arr[5:5+6], empty)
	Tokenize(arr[5:5+7], empty)
	//
	arr = getString(66, '-')
	arr[63] = '1'
	arr[65] = '2'
	Tokenize(arr, f)
	//
	arr = getString(66, '-')
	arr[63] = '1'
	arr[64] = '2'
	Tokenize(arr, f)
}

func getString(l int, c byte) []byte {
	arr := make([]byte, l)
	for i := 0; i < l; i++ {
		arr[i] = c
	}
	return arr
}

//func Test_segment2(t *testing.T) {
//	findSegment2(0b01)
//	findSegment2(0b0101)
//	findSegment2(uint64(1) << 63)
//}

/*
go test -benchmem -v -run=^$ -bench ^BenchmarkTokenize github.com/VictoriaMetrics/VictoriaMetrics/lib/stringsutil
1318.40 MB/s
1317.83 MB/s

// 原版 3392.69 MB/s

1283.83 MB/s  内联一个函数
*/
func BenchmarkTokenize(b *testing.B) {
	data, err := os.ReadFile("../../test/data/rfc-ref.txt")
	if err != nil {
		b.Error(err)
		return
	}
	empty := func(s string, isUnicode bool) {}
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Tokenize(data, empty)
	}
}

/*
4685.61 MB/s
4721.47 MB/s  去掉了多于的检查
4910.76 MB/s  去掉数组越界检查
4968.99 MB/s  减少flags 的写
4961.00 MB/s  就算不写回内存，性能也仍然没有提升
*/
func Benchmark_stringToBitmapV0(b *testing.B) {
	data, err := os.ReadFile("../../test/data/rfc-ref.txt")
	if err != nil {
		b.Error(err)
		return
	}
	headLen := getHeadLenForAlign(data, 64)
	data = data[headLen:]
	tailLen := len(data) & 63
	data = data[:len(data)-tailLen]
	//
	blockCnt := (len(data) + 63) / 64
	bitmap := make([]byte, blockCnt*8+7)
	headLen = getHeadLenForAlign(bitmap, 8)
	bitmap = bitmap[headLen:]
	tailLen = len(bitmap) & 7
	bitmap = bitmap[:len(bitmap)-tailLen]
	//
	//blockCnt := (len(data) + 63) / 64
	unicodeFlags := make([]byte, (blockCnt+7)/8)
	//
	SetZero(bitmap)
	SetZero(unicodeFlags)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stringToBitmapV0(data, bitmap, unicodeFlags)
	}
}

func Test_stringToBitmapV0(t *testing.T) {
	data, err := os.ReadFile("../../test/data/rfc-ref.txt")
	if err != nil {
		t.Error(err)
		return
	}
	headLen := getHeadLenForAlign(data, 64)
	data = data[headLen:]
	tailLen := len(data) & 63
	data = data[:len(data)-tailLen]
	//
	blockCnt := (len(data) + 63) / 64
	bitmap := make([]byte, blockCnt*8+7)
	headLen = getHeadLenForAlign(bitmap, 8)
	bitmap = bitmap[headLen:]
	tailLen = len(bitmap) & 7
	bitmap = bitmap[:len(bitmap)-tailLen]
	//
	//blockCnt := (len(data) + 63) / 64
	unicodeFlags := make([]byte, (blockCnt+7)/8)
	//
	SetZero(bitmap)
	SetZero(unicodeFlags)
	stringToBitmapV0(data, bitmap, unicodeFlags)
}

// 3165.34 MB/s 负优化
func Benchmark_stringToBitmapV1(b *testing.B) {
	data, err := os.ReadFile("../../test/data/rfc-ref.txt")
	if err != nil {
		b.Error(err)
		return
	}
	headLen := getHeadLenForAlign(data, 64)
	data = data[headLen:]
	tailLen := len(data) & 63
	data = data[:len(data)-tailLen]
	//
	blockCnt := (len(data) + 63) / 64
	bitmap := make([]byte, blockCnt*8+7)
	headLen = getHeadLenForAlign(bitmap, 8)
	bitmap = bitmap[headLen:]
	tailLen = len(bitmap) & 7
	bitmap = bitmap[:len(bitmap)-tailLen]
	//
	//blockCnt := (len(data) + 63) / 64
	unicodeFlags := make([]byte, (blockCnt+7)/8)
	//
	SetZero(bitmap)
	SetZero(unicodeFlags)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stringToBitmapV1(data, bitmap, unicodeFlags)
	}
}

// 4940.75 MB/s 减少位运算，并未见明显提升
func Benchmark_stringToBitmapV2(b *testing.B) {
	data, err := os.ReadFile("../../test/data/rfc-ref.txt")
	if err != nil {
		b.Error(err)
		return
	}
	headLen := getHeadLenForAlign(data, 64)
	data = data[headLen:]
	tailLen := len(data) & 63
	data = data[:len(data)-tailLen]
	//
	blockCnt := (len(data) + 63) / 64
	bitmap := make([]byte, blockCnt*8+7)
	headLen = getHeadLenForAlign(bitmap, 8)
	bitmap = bitmap[headLen:]
	tailLen = len(bitmap) & 7
	bitmap = bitmap[:len(bitmap)-tailLen]
	//
	//blockCnt := (len(data) + 63) / 64
	unicodeFlags := make([]byte, (blockCnt+7)/8)
	//
	SetZero(bitmap)
	SetZero(unicodeFlags)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stringToBitmapV2(data, bitmap, unicodeFlags)
	}
}
