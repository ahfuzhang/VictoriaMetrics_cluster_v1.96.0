package encoding

import (
	"bytes"
	"fmt"
	"math/bits"
	"testing"
)

func TestMarshalUnmarshalUint16(t *testing.T) {
	testMarshalUnmarshalUint16(t, 0)
	testMarshalUnmarshalUint16(t, 1)
	testMarshalUnmarshalUint16(t, (1<<16)-1)
	testMarshalUnmarshalUint16(t, (1<<15)+1)
	testMarshalUnmarshalUint16(t, (1<<15)-1)
	testMarshalUnmarshalUint16(t, 1<<15)

	for i := uint16(0); i < 1e4; i++ {
		testMarshalUnmarshalUint16(t, i)
	}
}

func testMarshalUnmarshalUint16(t *testing.T, u uint16) {
	t.Helper()

	b := MarshalUint16(nil, u)
	if len(b) != 2 {
		t.Fatalf("unexpected b length: %d; expecting %d", len(b), 2)
	}
	uNew := UnmarshalUint16(b)
	if uNew != u {
		t.Fatalf("unexpected uNew from b=%x; got %d; expecting %d", b, uNew, u)
	}

	prefix := []byte{1, 2, 3}
	b1 := MarshalUint16(prefix, u)
	if string(b1[:len(prefix)]) != string(prefix) {
		t.Fatalf("unexpected prefix for u=%d; got\n%x; expecting\n%x", u, b1[:len(prefix)], prefix)
	}
	if string(b1[len(prefix):]) != string(b) {
		t.Fatalf("unexpected b for u=%d; got\n%x; expecting\n%x", u, b1[len(prefix):], b)
	}
}

func TestMarshalUnmarshalUint32(t *testing.T) {
	testMarshalUnmarshalUint32(t, 0)
	testMarshalUnmarshalUint32(t, 1)
	testMarshalUnmarshalUint32(t, (1<<32)-1)
	testMarshalUnmarshalUint32(t, (1<<31)+1)
	testMarshalUnmarshalUint32(t, (1<<31)-1)
	testMarshalUnmarshalUint32(t, 1<<31)

	for i := uint32(0); i < 1e4; i++ {
		testMarshalUnmarshalUint32(t, i)
	}
}

func testMarshalUnmarshalUint32(t *testing.T, u uint32) {
	t.Helper()

	b := MarshalUint32(nil, u)
	if len(b) != 4 {
		t.Fatalf("unexpected b length: %d; expecting %d", len(b), 4)
	}
	uNew := UnmarshalUint32(b)
	if uNew != u {
		t.Fatalf("unexpected uNew from b=%x; got %d; expecting %d", b, uNew, u)
	}

	prefix := []byte{1, 2, 3}
	b1 := MarshalUint32(prefix, u)
	if string(b1[:len(prefix)]) != string(prefix) {
		t.Fatalf("unexpected prefix for u=%d; got\n%x; expecting\n%x", u, b1[:len(prefix)], prefix)
	}
	if string(b1[len(prefix):]) != string(b) {
		t.Fatalf("unexpected b for u=%d; got\n%x; expecting\n%x", u, b1[len(prefix):], b)
	}
}

func TestMarshalUnmarshalUint64(t *testing.T) {
	testMarshalUnmarshalUint64(t, 0)
	testMarshalUnmarshalUint64(t, 1)
	testMarshalUnmarshalUint64(t, (1<<64)-1)
	testMarshalUnmarshalUint64(t, (1<<63)+1)
	testMarshalUnmarshalUint64(t, (1<<63)-1)
	testMarshalUnmarshalUint64(t, 1<<63)

	for i := uint64(0); i < 1e4; i++ {
		testMarshalUnmarshalUint64(t, i)
	}
}

func testMarshalUnmarshalUint64(t *testing.T, u uint64) {
	t.Helper()

	b := MarshalUint64(nil, u)
	if len(b) != 8 {
		t.Fatalf("unexpected b length: %d; expecting %d", len(b), 8)
	}
	uNew := UnmarshalUint64(b)
	if uNew != u {
		t.Fatalf("unexpected uNew from b=%x; got %d; expecting %d", b, uNew, u)
	}

	prefix := []byte{1, 2, 3}
	b1 := MarshalUint64(prefix, u)
	if string(b1[:len(prefix)]) != string(prefix) {
		t.Fatalf("unexpected prefix for u=%d; got\n%x; expecting\n%x", u, b1[:len(prefix)], prefix)
	}
	if string(b1[len(prefix):]) != string(b) {
		t.Fatalf("unexpected b for u=%d; got\n%x; expecting\n%x", u, b1[len(prefix):], b)
	}
}

func TestMarshalUnmarshalInt16(t *testing.T) {
	testMarshalUnmarshalInt16(t, 0)
	testMarshalUnmarshalInt16(t, 1)
	testMarshalUnmarshalInt16(t, -1)
	testMarshalUnmarshalInt16(t, -1<<15)
	testMarshalUnmarshalInt16(t, (-1<<15)+1)
	testMarshalUnmarshalInt16(t, (1<<15)-1)

	for i := int16(0); i < 1e4; i++ {
		testMarshalUnmarshalInt16(t, i)
		testMarshalUnmarshalInt16(t, -i)
	}
}

func testMarshalUnmarshalInt16(t *testing.T, v int16) {
	t.Helper()

	b := MarshalInt16(nil, v)
	if len(b) != 2 {
		t.Fatalf("unexpected b length: %d; expecting %d", len(b), 2)
	}
	vNew := UnmarshalInt16(b)
	if vNew != v {
		t.Fatalf("unexpected vNew from b=%x; got %d; expecting %d", b, vNew, v)
	}

	prefix := []byte{1, 2, 3}
	b1 := MarshalInt16(prefix, v)
	if string(b1[:len(prefix)]) != string(prefix) {
		t.Fatalf("unexpected prefix for v=%d; got\n%x; expecting\n%x", v, b1[:len(prefix)], prefix)
	}
	if string(b1[len(prefix):]) != string(b) {
		t.Fatalf("unexpected b for v=%d; got\n%x; expecting\n%x", v, b1[len(prefix):], b)
	}
}

func TestMarshalUnmarshalInt64(t *testing.T) {
	testMarshalUnmarshalInt64(t, 0)
	testMarshalUnmarshalInt64(t, 1)
	testMarshalUnmarshalInt64(t, -1)
	testMarshalUnmarshalInt64(t, -1<<63)
	testMarshalUnmarshalInt64(t, (-1<<63)+1)
	testMarshalUnmarshalInt64(t, (1<<63)-1)

	for i := int64(0); i < 1e4; i++ {
		testMarshalUnmarshalInt64(t, i)
		testMarshalUnmarshalInt64(t, -i)
	}
}

func testMarshalUnmarshalInt64(t *testing.T, v int64) {
	t.Helper()

	b := MarshalInt64(nil, v)
	if len(b) != 8 {
		t.Fatalf("unexpected b length: %d; expecting %d", len(b), 8)
	}
	vNew := UnmarshalInt64(b)
	if vNew != v {
		t.Fatalf("unexpected vNew from b=%x; got %d; expecting %d", b, vNew, v)
	}

	prefix := []byte{1, 2, 3}
	b1 := MarshalInt64(prefix, v)
	if string(b1[:len(prefix)]) != string(prefix) {
		t.Fatalf("unexpected prefix for v=%d; got\n%x; expecting\n%x", v, b1[:len(prefix)], prefix)
	}
	if string(b1[len(prefix):]) != string(b) {
		t.Fatalf("unexpected b for v=%d; got\n%x; expecting\n%x", v, b1[len(prefix):], b)
	}
}

func TestMarshalUnmarshalVarInt64(t *testing.T) {
	testMarshalUnmarshalVarInt64(t, 0)
	testMarshalUnmarshalVarInt64(t, 1)
	testMarshalUnmarshalVarInt64(t, -1)
	testMarshalUnmarshalVarInt64(t, -1<<63)
	testMarshalUnmarshalVarInt64(t, (-1<<63)+1)
	testMarshalUnmarshalVarInt64(t, (1<<63)-1)

	for i := int64(0); i < 1e4; i++ {
		testMarshalUnmarshalVarInt64(t, i)
		testMarshalUnmarshalVarInt64(t, -i)
		testMarshalUnmarshalVarInt64(t, i<<8)
		testMarshalUnmarshalVarInt64(t, -i<<8)
		testMarshalUnmarshalVarInt64(t, i<<16)
		testMarshalUnmarshalVarInt64(t, -i<<16)
		testMarshalUnmarshalVarInt64(t, i<<23)
		testMarshalUnmarshalVarInt64(t, -i<<23)
		testMarshalUnmarshalVarInt64(t, i<<33)
		testMarshalUnmarshalVarInt64(t, -i<<33)
		testMarshalUnmarshalVarInt64(t, i<<43)
		testMarshalUnmarshalVarInt64(t, -i<<43)
		testMarshalUnmarshalVarInt64(t, i<<53)
		testMarshalUnmarshalVarInt64(t, -i<<53)
	}
}

func testMarshalUnmarshalVarInt64(t *testing.T, v int64) {
	t.Helper()

	//b := MarshalVarInt64(nil, v)
	b := MarshalVarInt64V11(nil, v)
	tail, vNew, err := UnmarshalVarInt64(b)
	if err != nil {
		t.Fatalf("unexpected error when unmarshaling v=%d from b=%x: %s", v, b, err)
	}
	if vNew != v {
		t.Fatalf("unexpected vNew from b=%x; got %d; expecting %d", b, vNew, v)
	}
	if len(tail) > 0 {
		t.Fatalf("unexpected data left after unmarshaling v=%d from b=%x: %x", v, b, tail)
	}

	prefix := []byte{1, 2, 3}
	//b1 := MarshalVarInt64(prefix, v)
	b1 := MarshalVarInt64V11(prefix, v)
	if string(b1[:len(prefix)]) != string(prefix) {
		t.Fatalf("unexpected prefix for v=%d; got\n%x; expecting\n%x", v, b1[:len(prefix)], prefix)
	}
	if string(b1[len(prefix):]) != string(b) {
		t.Fatalf("unexpected b for v=%d; got\n%x; expecting\n%x", v, b1[len(prefix):], b)
	}
}

func TestMarshalUnmarshalVarUint64(t *testing.T) {
	testMarshalUnmarshalVarUint64(t, 0)
	testMarshalUnmarshalVarUint64(t, 1)
	testMarshalUnmarshalVarUint64(t, (1<<63)-1)

	for i := uint64(0); i < 1024; i++ {
		testMarshalUnmarshalVarUint64(t, i)
		testMarshalUnmarshalVarUint64(t, i<<8)
		testMarshalUnmarshalVarUint64(t, i<<16)
		testMarshalUnmarshalVarUint64(t, i<<23)
		testMarshalUnmarshalVarUint64(t, i<<33)
		testMarshalUnmarshalVarUint64(t, i<<41)
		testMarshalUnmarshalVarUint64(t, i<<49)
		testMarshalUnmarshalVarUint64(t, i<<54)
	}
}

func testMarshalUnmarshalVarUint64(t *testing.T, u uint64) {
	t.Helper()

	b := MarshalVarUint64(nil, u)
	tail, uNew, err := UnmarshalVarUint64(b)
	if err != nil {
		t.Fatalf("unexpected error when unmarshaling u=%d from b=%x: %s", u, b, err)
	}
	if uNew != u {
		t.Fatalf("unexpected uNew from b=%x; got %d; expecting %d", b, uNew, u)
	}
	if len(tail) > 0 {
		t.Fatalf("unexpected data left after unmarshaling u=%d from b=%x: %x", u, b, tail)
	}

	prefix := []byte{1, 2, 3}
	b1 := MarshalVarUint64(prefix, u)
	if string(b1[:len(prefix)]) != string(prefix) {
		t.Fatalf("unexpected prefix for u=%d; got\n%x; expecting\n%x", u, b1[:len(prefix)], prefix)
	}
	if string(b1[len(prefix):]) != string(b) {
		t.Fatalf("unexpected b for u=%d; got\n%x; expecting\n%x", u, b1[len(prefix):], b)
	}
}

func TestMarshalUnmarshalBytes(t *testing.T) {
	testMarshalUnmarshalBytes(t, "")
	testMarshalUnmarshalBytes(t, "x")
	testMarshalUnmarshalBytes(t, "xy")

	var bb bytes.Buffer
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&bb, " %d ", i)
		s := bb.String()
		testMarshalUnmarshalBytes(t, s)
	}
}

func testMarshalUnmarshalBytes(t *testing.T, s string) {
	t.Helper()

	b := MarshalBytes(nil, []byte(s))
	tail, bNew, err := UnmarshalBytes(b)
	if err != nil {
		t.Fatalf("unexpected error when unmarshaling s=%q from b=%x: %s", s, b, err)
	}
	if string(bNew) != s {
		t.Fatalf("unexpected sNew from b=%x; got %q; expecting %q", b, bNew, s)
	}
	if len(tail) > 0 {
		t.Fatalf("unexepcted data left after unmarshaling s=%q from b=%x: %x", s, b, tail)
	}

	prefix := []byte("abcde")
	b1 := MarshalBytes(prefix, []byte(s))
	if string(b1[:len(prefix)]) != string(prefix) {
		t.Fatalf("unexpected prefix for s=%q; got\n%x; expecting\n%x", s, b1[:len(prefix)], prefix)
	}
	if string(b1[len(prefix):]) != string(b) {
		t.Fatalf("unexpected b for s=%q; got\n%x; expecting\n%x", s, b1[len(prefix):], b)
	}
}

func Test_MarshalVarInt64s_1(t *testing.T) {
	var dst [1024]byte
	var dst2 [1024]byte
	var values [1]int64
	prevLen := 0
	for i := 0; i < 1048576+1; i++ {
		values[0] = int64(i)
		result := MarshalVarInt64s(dst[:0], values[:1])
		if len(result) != prevLen {
			prevLen = len(result)
			t.Logf("MarshalVarInt64s len=%d, value=%d, result=%X", prevLen, i, result)
		}
		result2 := MarshalVarInt64BySearchTable(dst2[:0], int64(i))
		if !bytes.Equal(result, result2) {
			t.Errorf("MarshalVarInt64BySearchTable error")
			return
		}
	}
}

func Test_MarshalVarInt64s_2(t *testing.T) {
	var dst [1024]byte
	//var dst2 [1024]byte
	var values [1]int64
	prevLen := 0
	var cnt int
	for i := -1; i > -10000000; i-- {
		cnt++
		values[0] = int64(i)
		result := MarshalVarInt64s(dst[:0], values[:1])
		if len(result) != prevLen {
			prevLen = len(result)
			t.Logf("MarshalVarInt64s len=%d, value=%d(%d), result=%X", prevLen, i, cnt, result)
		}
	}
}

/*
	v = (v << 1) ^ (v >> 63) // zig-zag encoding without branching.
	u := uint64(v)
	for u > 0x7f {
		dst = append(dst, 0x80|byte(u))
		u >>= 7
	}
	dst = append(dst, byte(u))
*/

func Test_ZigZag1(t *testing.T) {
	var dst [128]byte
	var temp [128]byte
	var temp2 [128]byte
	var values [1]int64
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
	for _, v := range datas {
		n := ZigzagDecode(v)
		values[0] = n
		dst1 := MarshalVarInt64s(dst[:0], values[:1])
		dst2 := MarshalVarInt64V9(temp[:0], n)
		if !bytes.Equal(dst1, dst2) {
			t.Errorf("error, value=%d, right=%X, wrong=%X", n, dst1, dst2)
			return
		}
		dst3 := MarshalVarInt64sV12(temp2[:0], values[:1])
		if !bytes.Equal(dst1, dst3) {
			t.Errorf("error, value=%d, right=%X, wrong=%X", n, dst1, dst3)
			return
		}
	}
}

func Test_leadingZero(t *testing.T) {
	t.Logf("%d", bits.LeadingZeros64(0xFFFFFFFFFFFFFFFF))
	t.Logf("%d", bits.LeadingZeros64(0xFFFFFFFFFFFFFFFF-1))
	t.Logf("%d", bits.LeadingZeros64(0xEFFFFFFFFFFFFFFF))
	t.Logf("%d", bits.LeadingZeros64(0xDFFFFFFFFFFFFFFF))
	t.Logf("%d", bits.LeadingZeros64(0xCFFFFFFFFFFFFFFF))
	t.Logf("%d", bits.LeadingZeros64(0xCFFFFFFFFFFFFFFF)) // 0
	t.Logf("%d", bits.LeadingZeros64(0x7FFFFFFFFFFFFFFF)) // 1
	t.Logf("%d", bits.LeadingZeros64(0x3FFFFFFFFFFFFFFF)) // 2
	t.Logf("%d", bits.LeadingZeros64(0x1FFFFFFFFFFFFFFF)) // 3
	t.Logf("%d", bits.LeadingZeros64(0x0FFFFFFFFFFFFFFF)) // 4
}
