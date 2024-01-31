package encoding

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"sync"
)

// MarshalUint16 appends marshaled v to dst and returns the result.
func MarshalUint16(dst []byte, u uint16) []byte {
	return append(dst, byte(u>>8), byte(u))
}

// UnmarshalUint16 returns unmarshaled uint16 from src.
func UnmarshalUint16(src []byte) uint16 {
	// This is faster than the manual conversion.
	return binary.BigEndian.Uint16(src[:2])
}

// MarshalUint32 appends marshaled v to dst and returns the result.
func MarshalUint32(dst []byte, u uint32) []byte {
	return append(dst, byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

// UnmarshalUint32 returns unmarshaled uint32 from src.
func UnmarshalUint32(src []byte) uint32 {
	// This is faster than the manual conversion.
	return binary.BigEndian.Uint32(src[:4])
}

// MarshalUint64 appends marshaled v to dst and returns the result.
func MarshalUint64(dst []byte, u uint64) []byte {
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

// UnmarshalUint64 returns unmarshaled uint64 from src.
func UnmarshalUint64(src []byte) uint64 {
	// This is faster than the manual conversion.
	return binary.BigEndian.Uint64(src[:8])
}

// MarshalInt16 appends marshaled v to dst and returns the result.
func MarshalInt16(dst []byte, v int16) []byte {
	// Such encoding for negative v must improve compression.
	v = (v << 1) ^ (v >> 15) // zig-zag encoding without branching.
	u := uint16(v)
	return append(dst, byte(u>>8), byte(u))
}

// UnmarshalInt16 returns unmarshaled int16 from src.
func UnmarshalInt16(src []byte) int16 {
	// This is faster than the manual conversion.
	u := binary.BigEndian.Uint16(src[:2])
	v := int16(u>>1) ^ (int16(u<<15) >> 15) // zig-zag decoding without branching.
	return v
}

// MarshalInt64 appends marshaled v to dst and returns the result.
func MarshalInt64(dst []byte, v int64) []byte {
	// Such encoding for negative v must improve compression.
	v = (v << 1) ^ (v >> 63) // zig-zag encoding without branching.
	u := uint64(v)
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

// UnmarshalInt64 returns unmarshaled int64 from src.
func UnmarshalInt64(src []byte) int64 {
	// This is faster than the manual conversion.
	u := binary.BigEndian.Uint64(src[:8])
	v := int64(u>>1) ^ (int64(u<<63) >> 63) // zig-zag decoding without branching.
	return v
}

// MarshalVarInt64 appends marshalsed v to dst and returns the result.
func MarshalVarInt64(dst []byte, v int64) []byte {
	var tmp [1]int64
	tmp[0] = v
	return MarshalVarInt64s(dst, tmp[:])
}

// MarshalVarInt64s appends marshaled vs to dst and returns the result.
func MarshalVarInt64sV0(dst []byte, vs []int64) []byte { // 这个函数很慢
	for _, v := range vs {
		if v < 0x40 && v > -0x40 { // -65 < v <64 // 仅需要编码成一个字节的情况
			// Fast path
			c := int8(v)
			v := (c << 1) ^ (c >> 7) // zig-zag encoding without branching.
			dst = append(dst, byte(v))
			continue
		}

		v = (v << 1) ^ (v >> 63) // zig-zag encoding without branching.
		u := uint64(v)
		for u > 0x7f {
			dst = append(dst, 0x80|byte(u))
			u >>= 7
		}
		dst = append(dst, byte(u))
	}
	return dst
}

func MarshalVarInt64BySearchTable(dst []byte, v int64) []byte {
	if v <= ValueOf1Byte {
		return append(dst, MarshalVarInt64sTable[v])
	}
	if v <= ValueOf2Byte {
		idx := (ValueOf1Byte + 1) + (v-(ValueOf1Byte+1))*2
		return append(dst, MarshalVarInt64sTable[idx:idx+2]...)
	}
	if v <= ValueOf3Byte {
		idx := (ValueOf1Byte + 1) + (ValueOf2Byte-ValueOf1Byte)*2 + (v-(ValueOf2Byte+1))*3
		return append(dst, MarshalVarInt64sTable[idx:idx+3]...)
	}
	var arr [1]int64
	arr[0] = v
	return MarshalVarInt64s(dst, arr[:1])
}

func MarshalVarInt64sBySearchTable(dst []byte, vs []int64) []byte {
	var arr [1]int64
	for _, v := range vs {
		if v >= 0 {
			if v <= ValueOf1Byte {
				dst = append(dst, MarshalVarInt64sTable[v])
				continue
			}
			if v <= ValueOf2Byte {
				idx := (ValueOf1Byte + 1) + (v-(ValueOf1Byte+1))*2
				dst = append(dst, MarshalVarInt64sTable[idx:idx+2]...)
				continue
			}
			if v <= ValueOf3Byte {
				idx := (ValueOf1Byte + 1) + (ValueOf2Byte-ValueOf1Byte)*2 + (v-(ValueOf2Byte+1))*3
				dst = append(dst, MarshalVarInt64sTable[idx:idx+3]...)
				continue
			}
		}
		arr[0] = v
		dst = MarshalVarInt64s(dst, arr[:1])
	}
	return dst
}

const (
	ValueOf1Byte    = 64 - 1
	ValueOf2Byte    = 8192 - 1
	ValueOf3Byte    = 1048576 - 1
	totalTableBytes = (ValueOf1Byte+1)*1 + (ValueOf2Byte-ValueOf1Byte)*2 + (ValueOf3Byte-ValueOf2Byte)*3
	// 64 + (8192 - 64)*2 + (1048576-8192)*3 = 3137472 => 2.99 mb
)

var MarshalVarInt64sTable []byte = func() []byte {
	//
	table := make([]byte, 0, totalTableBytes)
	var values [1]int64
	for i := 0; i < ValueOf3Byte+1; i++ {
		values[0] = int64(i)
		table = MarshalVarInt64s(table, values[:1])
	}
	if len(table) != totalTableBytes {
		panic("error")
	}
	return table
}()

// UnmarshalVarInt64 returns unmarshaled int64 from src and returns
// the remaining tail from src.
func UnmarshalVarInt64(src []byte) ([]byte, int64, error) {
	var tmp [1]int64
	tail, err := UnmarshalVarInt64s(tmp[:], src)
	return tail, tmp[0], err
}

// UnmarshalVarInt64s unmarshals len(dst) int64 values from src to dst
// and returns the remaining tail from src.
func UnmarshalVarInt64s(dst []int64, src []byte) ([]byte, error) { // 这一个函数占了 1%， 需要优化 !!!
	idx := uint(0)
	for i := range dst { // 解码多少个 int64
		if idx >= uint(len(src)) {
			return nil, fmt.Errorf("cannot unmarshal varint from empty data")
		}
		c := src[idx]
		idx++
		if c < 0x80 {
			// Fast path
			v := int8(c>>1) ^ (int8(c<<7) >> 7) // zig-zag decoding without branching.
			dst[i] = int64(v)
			continue
		}

		// Slow path
		u := uint64(c & 0x7f)
		startIdx := idx - 1
		shift := uint8(0)
		for c >= 0x80 { //  !!! 无法预知数据有多长，无法加速
			if idx >= uint(len(src)) {
				return nil, fmt.Errorf("unexpected end of encoded varint at byte %d; src=%x", idx-startIdx, src[startIdx:])
			}
			if idx-startIdx > 9 {
				return src[idx:], fmt.Errorf("too long encoded varint; the maximum allowed length is 10 bytes; got %d bytes; src=%x",
					(idx-startIdx)+1, src[startIdx:])
			}
			c = src[idx]
			idx++
			shift += 7
			u |= uint64(c&0x7f) << shift
		}
		v := int64(u>>1) ^ (int64(u<<63) >> 63) // zig-zag decoding without branching.
		dst[i] = v
	}
	return src[idx:], nil
}

func UnmarshalVarInt64ForOne(buf []byte) int64 {
	var n uint64
	switch len(buf) {
	case 1:
		n = uint64(buf[0] & 0x7F)
	case 2:
		n = uint64(buf[0]&0x7F) | (uint64(buf[1]&0x7F) << 7)
	case 3:
		n = uint64(buf[0]&0x7F) | (uint64(buf[1]&0x7F) << 7) | (uint64(buf[2]&0x7F) << 14)
	case 4:
		n = uint64(buf[0]&0x7F) | (uint64(buf[1]&0x7F) << 7) | (uint64(buf[2]&0x7F) << 14) | (uint64(buf[3]&0x7F) << 21)
	case 5:
		n = uint64(buf[0]&0x7F) | (uint64(buf[1]&0x7F) << 7) | (uint64(buf[2]&0x7F) << 14) | (uint64(buf[3]&0x7F) << 21) | (uint64(buf[4]&0x7F) << 28)
	case 6:
		n = uint64(buf[0]&0x7F) | (uint64(buf[1]&0x7F) << 7) | (uint64(buf[2]&0x7F) << 14) | (uint64(buf[3]&0x7F) << 21) | (uint64(buf[4]&0x7F) << 28) | (uint64(buf[5]&0x7F) << 35)
	case 7:
		n = uint64(buf[0]&0x7F) | (uint64(buf[1]&0x7F) << 7) | (uint64(buf[2]&0x7F) << 14) | (uint64(buf[3]&0x7F) << 21) | (uint64(buf[4]&0x7F) << 28) | (uint64(buf[5]&0x7F) << 35) | (uint64(buf[6]&0x7F) << 42)
	case 8:
		n = uint64(buf[0]&0x7F) | (uint64(buf[1]&0x7F) << 7) | (uint64(buf[2]&0x7F) << 14) | (uint64(buf[3]&0x7F) << 21) | (uint64(buf[4]&0x7F) << 28) | (uint64(buf[5]&0x7F) << 35) | (uint64(buf[6]&0x7F) << 42) | (uint64(buf[7]&0x7F) << 49)
	case 9:
		n = uint64(buf[0]&0x7F) | (uint64(buf[1]&0x7F) << 7) | (uint64(buf[2]&0x7F) << 14) | (uint64(buf[3]&0x7F) << 21) | (uint64(buf[4]&0x7F) << 28) | (uint64(buf[5]&0x7F) << 35) | (uint64(buf[6]&0x7F) << 42) | (uint64(buf[7]&0x7F) << 49) | (uint64(buf[8]&0x7F) << 56)
	case 10:
		n = uint64(buf[0]&0x7F) | (uint64(buf[1]&0x7F) << 7) | (uint64(buf[2]&0x7F) << 14) | (uint64(buf[3]&0x7F) << 21) | (uint64(buf[4]&0x7F) << 28) | (uint64(buf[5]&0x7F) << 35) | (uint64(buf[6]&0x7F) << 42) | (uint64(buf[7]&0x7F) << 49) | (uint64(buf[8]&0x7F) << 56) | (uint64(buf[9]&0x7F) << 63)
	default:
		panic("impossible error: buf length must be 1 to 10")
	}
	return int64(n>>1) ^ (int64(n<<63) >> 63)
}

// 解码 n 个 int64
// 返回 tail
func UnmarshalVarInt64sV1(dst []int64, src []byte) ([]byte, error) {
	idx := uint(0)
	for i := range dst { // 解码多少个 int64
		if idx >= uint(len(src)) {
			return nil, fmt.Errorf("cannot unmarshal varint from empty data")
		}
		c := src[idx]
		idx++
		if c < 0x80 {
			// Fast path
			v := int8(c>>1) ^ (int8(c<<7) >> 7) // zig-zag decoding without branching.
			dst[i] = int64(v)
			continue
		}
		// 对 2 字节的情况进行加速
		if idx < uint(len(src)) && src[idx] < 0x80 {
			n := uint64(c&0x7f) | (uint64(src[idx]&0x7f) << 7)
			dst[i] = int64(n>>1) ^ (int64(n<<63) >> 63)
			idx++
			continue
		}
		// 查找结束位置
		j := idx + 1
		// if uint(len(src))-j >= 8 {
		// 	if src[j] < 0x80 {
		// 		j += 0
		// 	} else if src[j+1] < 0x80 {
		// 		j += 1
		// 	} else if src[j+2] < 0x80 {
		// 		j += 2
		// 	} else if src[j+3] < 0x80 {
		// 		j += 3
		// 	} else if src[j+4] < 0x80 {
		// 		j += 4
		// 	} else if src[j+5] < 0x80 {
		// 		j += 5
		// 	} else if src[j+6] < 0x80 {
		// 		j += 6
		// 	} else if src[j+7] < 0x80 {
		// 		j += 7
		// 	} else {
		// 		return nil, fmt.Errorf("cannot unmarshal varint, not found end at %d", j+7)
		// 	}
		// } else {
		// 	for ; j < uint(len(src)); j++ {
		// 		if src[j] < 0x80 {
		// 			break
		// 		}
		// 	}
		// }
		for ; j < uint(len(src)); j++ {
			if src[j] < 0x80 {
				break
			}
		}
		//bufLen :=
		if j-idx > 10 {
			return nil, fmt.Errorf("cannot unmarshal varint, buffer too long, len=%d", j-idx)
		}
		dst[i] = UnmarshalVarInt64ForOne(src[idx-1 : j+1])
		idx = j + 1
	}
	return src[idx:], nil
}

func UnmarshalVarInt64V1(src []byte) ([]byte, int64, error) {
	var tmp [1]int64
	tail, err := UnmarshalVarInt64sV1(tmp[:], src)
	return tail, tmp[0], err
}

// MarshalVarUint64 appends marshaled u to dst and returns the result.
func MarshalVarUint64(dst []byte, u uint64) []byte {
	var tmp [1]uint64
	tmp[0] = u
	return MarshalVarUint64s(dst, tmp[:])
}

// MarshalVarUint64s appends marshaled us to dst and returns the result.
func MarshalVarUint64s(dst []byte, us []uint64) []byte {
	for _, u := range us {
		if u < 0x80 {
			// Fast path
			dst = append(dst, byte(u))
			continue
		}
		for u > 0x7f {
			dst = append(dst, 0x80|byte(u))
			u >>= 7
		}
		dst = append(dst, byte(u))
	}
	return dst
}

// UnmarshalVarUint64 returns unmarshaled uint64 from src and returns
// the remaining tail from src.
func UnmarshalVarUint64(src []byte) ([]byte, uint64, error) {
	var tmp [1]uint64
	tail, err := UnmarshalVarUint64s(tmp[:], src)
	return tail, tmp[0], err
}

// UnmarshalVarUint64s unmarshals len(dst) uint64 values from src to dst
// and returns the remaining tail from src.
func UnmarshalVarUint64s(dst []uint64, src []byte) ([]byte, error) {
	idx := uint(0)
	for i := range dst {
		if idx >= uint(len(src)) {
			return nil, fmt.Errorf("cannot unmarshal varuint from empty data")
		}
		c := src[idx]
		idx++
		if c < 0x80 {
			// Fast path
			dst[i] = uint64(c)
			continue
		}

		// Slow path
		u := uint64(c & 0x7f)
		startIdx := idx - 1
		shift := uint8(0)
		for c >= 0x80 {
			if idx >= uint(len(src)) {
				return nil, fmt.Errorf("unexpected end of encoded varint at byte %d; src=%x", idx-startIdx, src[startIdx:])
			}
			if idx-startIdx > 9 {
				return src[idx:], fmt.Errorf("too long encoded varint; the maximum allowed length is 10 bytes; got %d bytes; src=%x",
					(idx-startIdx)+1, src[startIdx:])
			}
			c = src[idx]
			idx++
			shift += 7
			u |= uint64(c&0x7f) << shift
		}
		dst[i] = u
	}
	return src[idx:], nil
}

// MarshalBool appends marshaled v to dst and returns the result.
func MarshalBool(dst []byte, v bool) []byte {
	x := byte(0)
	if v {
		x = 1
	}
	return append(dst, x)
}

// UnmarshalBool unmarshals bool from src.
func UnmarshalBool(src []byte) bool {
	return src[0] != 0
}

// MarshalBytes appends marshaled b to dst and returns the result.
func MarshalBytes(dst, b []byte) []byte {
	dst = MarshalVarUint64(dst, uint64(len(b)))
	dst = append(dst, b...)
	return dst
}

// UnmarshalBytes returns unmarshaled bytes from src.
func UnmarshalBytes(src []byte) ([]byte, []byte, error) {
	tail, n, err := UnmarshalVarUint64(src)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot unmarshal string size: %w", err)
	}
	src = tail
	if uint64(len(src)) < n {
		return nil, nil, fmt.Errorf("src is too short for reading string with size %d; len(src)=%d", n, len(src))
	}
	return src[n:], src[:n], nil
}

// GetInt64s returns an int64 slice with the given size.
// The slice contents isn't initialized - it may contain garbage.
func GetInt64s(size int) *Int64s {
	v := int64sPool.Get()
	if v == nil {
		return &Int64s{
			A: make([]int64, size),
		}
	}
	is := v.(*Int64s)
	if n := size - cap(is.A); n > 0 {
		is.A = append(is.A[:cap(is.A)], make([]int64, n)...)
	}
	is.A = is.A[:size]
	return is
}

// PutInt64s returns is to the pool.
func PutInt64s(is *Int64s) {
	int64sPool.Put(is)
}

// Int64s holds an int64 slice
type Int64s struct {
	A []int64
}

var int64sPool sync.Pool

// GetUint64s returns an uint64 slice with the given size.
// The slice contents isn't initialized - it may contain garbage.
func GetUint64s(size int) *Uint64s {
	v := uint64sPool.Get()
	if v == nil {
		return &Uint64s{
			A: make([]uint64, size),
		}
	}
	is := v.(*Uint64s)
	if n := size - cap(is.A); n > 0 {
		is.A = append(is.A[:cap(is.A)], make([]uint64, n)...)
	}
	is.A = is.A[:size]
	return is
}

// PutUint64s returns is to the pool.
func PutUint64s(is *Uint64s) {
	uint64sPool.Put(is)
}

// Uint64s holds an uint64 slice
type Uint64s struct {
	A []uint64
}

var uint64sPool sync.Pool

// GetUint32s returns an uint32 slice with the given size.
// The slize contents isn't initialized - it may contain garbage.
func GetUint32s(size int) *Uint32s {
	v := uint32sPool.Get()
	if v == nil {
		return &Uint32s{
			A: make([]uint32, size),
		}
	}
	is := v.(*Uint32s)
	if n := size - cap(is.A); n > 0 {
		is.A = append(is.A[:cap(is.A)], make([]uint32, n)...)
	}
	is.A = is.A[:size]
	return is
}

// PutUint32s returns is to the pool.
func PutUint32s(is *Uint32s) {
	uint32sPool.Put(is)
}

// Uint32s holds an uint32 slice
type Uint32s struct {
	A []uint32
}

var uint32sPool sync.Pool

// ZigzagEncode 对 int64 进行 Zigzag 编码
func ZigzagEncode(n int64) uint64 {
	return uint64((n << 1) ^ (n >> 63))
}

// ZigzagDecode 对 uint64 进行 Zigzag 解码
func ZigzagDecode(n uint64) int64 {
	return int64((n >> 1) ^ (-(n & 1)))
}

const (
	UintRange7Bit  = uint64(1 << 7)
	UintRange14Bit = uint64(1 << 14)
	UintRange21Bit = uint64(1 << 21)
	UintRange28Bit = uint64(1 << 28)
	UintRange35Bit = uint64(1 << 35)
	UintRange42Bit = uint64(1 << 42)
	UintRange49Bit = uint64(1 << 49)
	UintRange56Bit = uint64(1 << 56)
	UintRange63Bit = uint64(1 << 63)
)

func MarshalVarInt64V9(dst []byte, v int64) []byte {
	n := uint64((v << 1) ^ (v >> 63))
	if n < UintRange7Bit {
		dst = append(dst, byte(n))
		return dst
	}
	if n < UintRange14Bit {
		dst = append(dst, byte(n|0x80), byte(n>>7))
		return dst
	}
	if n < UintRange21Bit {
		dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte(n>>14))
		return dst
	}
	if n < UintRange28Bit {
		dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte(n>>21))
		return dst
	}
	if n < UintRange28Bit {
		dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte(n>>21))
		return dst
	}
	if n < UintRange35Bit {
		dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28))
		return dst
	}
	if n < UintRange42Bit {
		dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35))
		return dst
	}
	if n < UintRange49Bit {
		dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42))
		return dst
	}
	if n < UintRange56Bit {
		dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42|0x80), byte(n>>49))
		return dst
	}
	if n < UintRange63Bit {
		dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42|0x80), byte(n>>49|0x80), byte(n>>56))
		return dst
	}
	dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42|0x80), byte(n>>49|0x80), byte(n>>56|0x80), byte(n>>63))
	return dst
}

func MarshalVarInt64sV9(dst []byte, vs []int64) []byte {
	for _, v := range vs {
		n := uint64((v << 1) ^ (v >> 63))
		if n < UintRange7Bit {
			dst = append(dst, byte(n))
			continue
		}
		if n < UintRange14Bit {
			dst = append(dst, byte(n|0x80), byte(n>>7))
			continue
		}
		if n < UintRange21Bit {
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte(n>>14))
			continue
		}
		if n < UintRange28Bit {
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte(n>>21))
			continue
		}
		if n < UintRange35Bit {
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28))
			continue
		}
		if n < UintRange42Bit {
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35))
			continue
		}
		if n < UintRange49Bit {
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42))
			continue
		}
		if n < UintRange56Bit {
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42|0x80), byte(n>>49))
			continue
		}
		if n < UintRange63Bit {
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42|0x80), byte(n>>49|0x80), byte(n>>56))
			continue
		}
		dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42|0x80), byte(n>>49|0x80), byte(n>>56|0x80), byte(n>>63))
		//return dst
	}
	return dst
}

func MarshalVarInt64s(dst []byte, vs []int64) []byte {
	for _, v := range vs {
		n := uint64((v << 1) ^ (v >> 63))
		if n < (1 << 7) {
			dst = append(dst, byte(n))
			continue
		}
		switch (64 - bits.LeadingZeros64(n>>1)) / 7 {
		case 0:
			dst = append(dst, byte(n))
		case 1:
			dst = append(dst, byte(n|0x80), byte(n>>7))
		case 2:
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte(n>>14))
		case 3:
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte(n>>21))
		case 4:
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28))
		case 5:
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35))
		case 6:
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42))
		case 7:
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42|0x80), byte(n>>49))
		case 8:
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42|0x80), byte(n>>49|0x80), byte(n>>56))
		case 9:
			fallthrough
		default:
			dst = append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42|0x80), byte(n>>49|0x80), byte(n>>56|0x80), byte(n>>63))
		}
	}
	return dst
}

func MarshalVarInt64V10(dst []byte, v int64) []byte {
	var arr [1]int64
	arr[0] = v
	return MarshalVarInt64s(dst, arr[:1])
}

var jumpTable = [10]func(dst []byte, v uint64) []byte{
	func(dst []byte, n uint64) []byte { //0
		return append(dst, byte(n))
	},
	func(dst []byte, n uint64) []byte { // 1
		return append(dst, byte(n|0x80), byte(n>>7))
	},
	func(dst []byte, n uint64) []byte { //2
		return append(dst, byte(n|0x80), byte((n>>7)|0x80), byte(n>>14))
	},
	func(dst []byte, n uint64) []byte { // 3
		return append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte(n>>21))
	},
	func(dst []byte, n uint64) []byte { // 4
		return append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28))
	},
	func(dst []byte, n uint64) []byte { // 5
		return append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35))
	},
	func(dst []byte, n uint64) []byte { // 6
		return append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42))
	},
	func(dst []byte, n uint64) []byte { // 7
		return append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42|0x80), byte(n>>49))
	},
	func(dst []byte, n uint64) []byte { // 8
		return append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42|0x80), byte(n>>49|0x80), byte(n>>56))
	},
	func(dst []byte, n uint64) []byte { // 9
		return append(dst, byte(n|0x80), byte((n>>7)|0x80), byte((n>>14)|0x80), byte((n>>21)|0x80), byte(n>>28|0x80), byte(n>>35|0x80), byte(n>>42|0x80), byte(n>>49|0x80), byte(n>>56|0x80), byte(n>>63))
	},
}

func MarshalVarInt64sV11(dst []byte, vs []int64) []byte {
	for _, v := range vs {
		n := uint64((v << 1) ^ (v >> 63))
		dst = jumpTable[(64-bits.LeadingZeros64(n>>1))/7](dst, n)
	}
	return dst
}

func MarshalVarInt64V11(dst []byte, v int64) []byte {
	var arr [1]int64
	arr[0] = v
	return MarshalVarInt64sV11(dst, arr[:1])
}
