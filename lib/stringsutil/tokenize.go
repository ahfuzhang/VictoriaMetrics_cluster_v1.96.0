package stringsutil

import (
	"math/bits"
	"unsafe"
)

// lookup table for ascii only
var asciiTable [256]byte = initAsciiTable()

func initAsciiTable() (table [256]byte) {
	for i := '0'; i <= '9'; i++ {
		table[i] = 1
	}
	for i := 'a'; i <= 'z'; i++ {
		table[i] = 1
	}
	for i := 'A'; i <= 'Z'; i++ {
		table[i] = 1
	}
	table['_'] = 1
	return
}

// IsASCII check string is ascii token only
func IsASCII(s string) bool {
	for i := range s {
		if asciiTable[s[i]] == 0 {
			return false
		}
	}
	return true
}

// lookup table for ascii token and unicode token
var unicodeTable [256]byte = initUnicodeTable()

func initUnicodeTable() (table [256]byte) {
	for i := '0'; i <= '9'; i++ {
		table[i] = 1
	}
	for i := 'a'; i <= 'z'; i++ {
		table[i] = 1
	}
	for i := 'A'; i <= 'Z'; i++ {
		table[i] = 1
	}
	table['_'] = 1
	for i := 128; i <= 255; i++ {
		table[i] = 1
	}
	return
}

var lookupTables [2][256]byte = func() [2][256]byte {
	return [2][256]byte{
		initAsciiTable(),
		initUnicodeTable(),
	}
}()

const asciiMask uint64 = 0x8080808080808080

func uint64ToMask(v uint64, table *[256]byte) uint8 {
	return uint8(
		((*table)[byte(v)]) |
			((*table)[byte(v>>8)] << 1) |
			((*table)[byte(v>>16)] << 2) |
			((*table)[byte(v>>24)] << 3) |
			((*table)[byte(v>>32)] << 4) |
			((*table)[byte(v>>40)] << 5) |
			((*table)[byte(v>>48)] << 6) |
			((*table)[byte(v>>56)] << 7))
	//chars := (*[8]uint8)(unsafe.Pointer(&v))
	//return (table[chars[0]]) |
	//	(table[chars[1]] << 1) |
	//	(table[chars[2]] << 2) |
	//	(table[chars[3]] << 3) |
	//	(table[chars[4]] << 4) |
	//	(table[chars[5]] << 5) |
	//	(table[chars[6]] << 6) |
	//	(table[chars[7]] << 7)
}

// 8 x uint64 to 64bit bitmap
//func uint64x8ToMask(a *[8]uint64, table *[256]byte) uint64 {
//	return uint64(uint64ToMask((*a)[0], table)) |
//		uint64(uint64ToMask((*a)[1], table))<<8 |
//		uint64(uint64ToMask((*a)[2], table))<<16 |
//		uint64(uint64ToMask((*a)[3], table))<<24 |
//		uint64(uint64ToMask((*a)[4], table))<<32 |
//		uint64(uint64ToMask((*a)[5], table))<<40 |
//		uint64(uint64ToMask((*a)[6], table))<<48 |
//		uint64(uint64ToMask((*a)[7], table))<<56
//}

//func batchToMask(batch *[8]uint64) (uint64, bool) {
//	v := batch[0] | batch[1] | batch[2] | batch[3] | batch[4] | batch[5] | batch[6] | batch[7]
//	v &= asciiMask
//	if v != 0 {
//		return uint64x8ToMask(batch, &unicodeTable), true
//	}
//	return uint64x8ToMask(batch, &asciiTable), false
//}

type TokenCallback func(s string, isUnicode bool)

type tokenizerState struct {
	prevStart          int
	prevUnicodeFlag    bool
	currentUnicodeFlag bool
	current            int
	currentLen         int
	callback           TokenCallback
	//b                  []byte
	ptr      unsafe.Pointer
	totalLen int
}

func (ts *tokenizerState) onTokenFound(start, end int, unicodeFlag bool) { // leaking param content: ts
	s := unsafe.String((*byte)(unsafe.Add(ts.ptr, start)), end-start)
	ts.callback(s, unicodeFlag)
}

func (ts *tokenizerState) splitByMask(mask uint64, isUnicode bool) {
	if mask == 0 {
		if ts.prevStart >= 0 {
			ts.onTokenFound(ts.prevStart, ts.current, ts.prevUnicodeFlag)
			ts.prevStart = -1
		}
		return
	}
	ts.currentUnicodeFlag = isUnicode
	ts.findSegment(mask)
	ts.current += ts.currentLen
}

func (ts *tokenizerState) end() {
	if ts.prevStart < 0 {
		return
	}
	ts.onTokenFound(ts.prevStart, ts.totalLen, ts.prevUnicodeFlag)
	ts.prevStart = -1
}

func (ts *tokenizerState) onFound(start, end int) {
	// 处理找到的起始位置
	if ts.prevStart >= 0 {
		if ts.current <= ts.prevStart {
			panic("ts.current<=ts.prevStart") //  "ts.current<=ts.prevStart" escapes to heap
		}
		// 如果有上次的位置
		if start == 0 {
			// 和上次的位置接上了
			if end == ts.currentLen {
				// 连续跨了 64 个字符
				ts.prevUnicodeFlag = ts.prevUnicodeFlag || ts.currentUnicodeFlag
				return
			}
			ts.onTokenFound(ts.prevStart, ts.current+end, ts.prevUnicodeFlag || ts.currentUnicodeFlag)
			ts.prevStart = -1
			return
		}
		// 字符串刚刚跨 64 个字符的边界
		ts.onTokenFound(ts.prevStart, ts.current, ts.prevUnicodeFlag)
		ts.prevStart = -1
		// 然后再处理当前这一段
		ts.onTokenFound(ts.current+start, ts.current+end, ts.currentUnicodeFlag)
		return
	}
	if end == ts.currentLen { // 后续标志提前被设置了，导致最后一条的逻辑影响了前面n 条
		// 最后一个 end 等于 64, 说明字符串可能没结束
		ts.prevStart = ts.current + start
		ts.prevUnicodeFlag = ts.currentUnicodeFlag
		return
	}
	ts.onTokenFound(ts.current+start, ts.current+end, ts.currentUnicodeFlag)
}

func (ts *tokenizerState) findSegment(bitmap uint64) {
	shifted := bitmap << 1
	starts := (^shifted) & bitmap
	ends := shifted & (^bitmap)
	for starts != 0 {
		// 找到最高位的1
		startBit := bits.TrailingZeros64(starts)
		// 清除当前的start位
		starts &^= (1 << startBit)
		// 找end
		endBit := bits.TrailingZeros64(ends)
		//if endBit == 0 {
		//	endBit = 64
		//	ts.onFound(startBit, endBit)
		//	return
		//}
		if endBit == 0 {
			panic("should be 64")
		}
		ts.onFound(startBit, endBit)
		// 清除当前end位
		ends &^= (1 << endBit)
	}
	return
}

// Tokenize tokenizes the Values
// @param b buffer for all strings, separated by '\0'
// @param callback the callback function to be called for each token
//
//go:nosplit
func Tokenize(b []byte, callback TokenCallback) {
	if len(b) == 0 {
		return
	}
	ptr := unsafe.Pointer(&b[0])
	addr := uint64(uintptr(ptr))
	alignAddr := (addr + uint64(63)) & (^uint64(63))
	headLen := alignAddr - addr
	//
	isEnd := false
	var processLen int
	var tempLen int
	var result uint64
	var result2 uint64
	//var moveBitsCnt int
	var align64Len int
	var leftLen int
	var uint64Cnt int
	var values *[8]uint64
	var chars *[8]uint8
	//var isUnicode bool
	var state = tokenizerState{
		callback: callback,
		//b:         b,
		ptr:       ptr,
		totalLen:  len(b),
		prevStart: -1,
	}
	//
	tempLen = (len(b) - int(headLen))
	align64Len = tempLen & (-64)
	leftLen = tempLen & 63
	if len(b) < 64 /* ||align64Len==0*/ {
		isEnd = true
		processLen = len(b)
		goto len_less_64
	}
	if len(b) >= 64 && align64Len == 0 {
		align64Len = len(b) & (-64)
		leftLen = len(b) & 63
		goto align64
	}
	if headLen == 0 {
		goto align64
	}
	processLen = int(headLen)
len_less_64:
	//isUnicode = true
	result = 0
	uint64Cnt = processLen >> 3 // processLen / 8
	values = (*[8]uint64)(ptr)
	switch uint64Cnt {
	case 1:
		result = uint64(uint64ToMask(values[0], &unicodeTable))
	case 2:
		result = uint64(uint64ToMask(values[0], &unicodeTable)) |
			(uint64(uint64ToMask(values[1], &unicodeTable)) << 8)
	case 3:
		result = uint64(uint64ToMask(values[0], &unicodeTable)) |
			(uint64(uint64ToMask(values[1], &unicodeTable)) << 8) |
			(uint64(uint64ToMask(values[2], &unicodeTable)) << 16)
	case 4:
		result = uint64(uint64ToMask(values[0], &unicodeTable)) |
			(uint64(uint64ToMask(values[1], &unicodeTable)) << 8) |
			(uint64(uint64ToMask(values[2], &unicodeTable)) << 16) |
			(uint64(uint64ToMask(values[3], &unicodeTable)) << 24)
	case 5:
		result = uint64(uint64ToMask(values[0], &unicodeTable)) |
			(uint64(uint64ToMask(values[1], &unicodeTable)) << 8) |
			(uint64(uint64ToMask(values[2], &unicodeTable)) << 16) |
			(uint64(uint64ToMask(values[3], &unicodeTable)) << 24) |
			(uint64(uint64ToMask(values[4], &unicodeTable)) << 32)
	case 6:
		result = uint64(uint64ToMask(values[0], &unicodeTable)) |
			(uint64(uint64ToMask(values[1], &unicodeTable)) << 8) |
			(uint64(uint64ToMask(values[2], &unicodeTable)) << 16) |
			(uint64(uint64ToMask(values[3], &unicodeTable)) << 24) |
			(uint64(uint64ToMask(values[4], &unicodeTable)) << 32) |
			(uint64(uint64ToMask(values[5], &unicodeTable)) << 40)
	case 7:
		result = uint64(uint64ToMask(values[0], &unicodeTable)) |
			(uint64(uint64ToMask(values[1], &unicodeTable)) << 8) |
			(uint64(uint64ToMask(values[2], &unicodeTable)) << 16) |
			(uint64(uint64ToMask(values[3], &unicodeTable)) << 24) |
			(uint64(uint64ToMask(values[4], &unicodeTable)) << 32) |
			(uint64(uint64ToMask(values[5], &unicodeTable)) << 40) |
			(uint64(uint64ToMask(values[6], &unicodeTable)) << 48)
	case 8:
		//result = uint64x8ToMask(values, &unicodeTable)
		//state.splitByMask(result, true, 64)
		//result = 0
		panic("impossible")
	}
	ptr = unsafe.Add(ptr, processLen&(-8))
	tempLen = processLen & 7
	if tempLen > 0 {
		//result <<= 8
		result2 = 0
		chars = (*[8]uint8)(ptr)
		switch tempLen {
		case 1:
			result2 = uint64(unicodeTable[chars[0]])
		case 2:
			result2 = (uint64(unicodeTable[chars[0]]) |
				(uint64(unicodeTable[chars[1]]) << 1))
		case 3:
			result2 = (uint64(unicodeTable[chars[0]]) |
				(uint64(unicodeTable[chars[1]]) << 1) |
				(uint64(unicodeTable[chars[2]]) << 2))
		case 4:
			result2 = (uint64(unicodeTable[chars[0]]) |
				(uint64(unicodeTable[chars[1]]) << 1) |
				(uint64(unicodeTable[chars[2]]) << 2) |
				(uint64(unicodeTable[chars[3]]) << 3))
		case 5:
			result2 = (uint64(unicodeTable[chars[0]]) |
				(uint64(unicodeTable[chars[1]]) << 1) |
				(uint64(unicodeTable[chars[2]]) << 2) |
				(uint64(unicodeTable[chars[3]]) << 3) |
				(uint64(unicodeTable[chars[4]]) << 4))
		case 6:
			result2 = (uint64(unicodeTable[chars[0]]) |
				(uint64(unicodeTable[chars[1]]) << 1) |
				(uint64(unicodeTable[chars[2]]) << 2) |
				(uint64(unicodeTable[chars[3]]) << 3) |
				(uint64(unicodeTable[chars[4]]) << 4) |
				(uint64(unicodeTable[chars[5]]) << 5))
		case 7:
			result2 = (uint64(unicodeTable[chars[0]]) |
				(uint64(unicodeTable[chars[1]]) << 1) |
				(uint64(unicodeTable[chars[2]]) << 2) |
				(uint64(unicodeTable[chars[3]]) << 3) |
				(uint64(unicodeTable[chars[4]]) << 4) |
				(uint64(unicodeTable[chars[5]]) << 5) |
				(uint64(unicodeTable[chars[6]]) << 6))
		case 8:
			//result |= uint64(uint64ToMask(*(*uint64)(ptr), &unicodeTable))
			panic("impossible")
		}
		ptr = unsafe.Add(ptr, tempLen)
		result2 <<= (uint64Cnt * 8)
		result |= result2
	}
	// 检查 bitmap
	state.currentLen = processLen
	state.splitByMask(result, true)
	if isEnd {
		state.end()
		return
	}
align64:
	state.currentLen = 64
	for i := 0; i < align64Len; i += 64 {
		values = ((*[8]uint64)(unsafe.Add(ptr, i)))
		v := values[0] | values[1] | values[2] | values[3] | values[4] | values[5] | values[6] | values[7]
		v &= asciiMask
		index := (v | -v) >> 63
		table := &lookupTables[index]
		result = uint64(uint64ToMask(values[0], table)) |
			(uint64(uint64ToMask(values[1], table)) << 8) |
			(uint64(uint64ToMask(values[2], table)) << 16) |
			(uint64(uint64ToMask(values[3], table)) << 24) |
			(uint64(uint64ToMask(values[4], table)) << 32) |
			(uint64(uint64ToMask(values[5], table)) << 40) |
			(uint64(uint64ToMask(values[6], table)) << 48) |
			(uint64(uint64ToMask(values[7], table)) << 56)
		isUnicode := v != 0
		//state.splitByMask(result, v != 0)
		if result == 0 {
			if state.prevStart >= 0 {
				state.onTokenFound(state.prevStart, state.current, state.prevUnicodeFlag)
				state.prevStart = -1
			}
			continue
		}
		state.currentUnicodeFlag = isUnicode
		state.findSegment(result)
		shifted := result << 1
		starts := (^shifted) & result
		ends := shifted & (^result)
		for starts != 0 {
			// 找到最高位的1
			startBit := bits.TrailingZeros64(starts)
			// 清除当前的start位
			starts &^= (1 << startBit)
			// 找end
			endBit := bits.TrailingZeros64(ends)
			//if endBit == 0 {
			//	endBit = 64
			//	ts.onFound(startBit, endBit)
			//	return
			//}
			if endBit == 0 {
				panic("should be 64")
			}
			state.onFound(startBit, endBit) // 就算去掉这行，性能也提升不多
			// 清除当前end位
			ends &^= (1 << endBit)
		}
		state.current += 64
	}
	isEnd = true
	ptr = unsafe.Add(ptr, align64Len)
	processLen = leftLen
	goto len_less_64
}

func stringToBitmapV0(b []byte, outBitmap []byte, outUnicodeFlags []byte) {
	//l := len(b)
	ptr := unsafe.Pointer(unsafe.SliceData(b))
	//addr := uint64(uintptr(ptr))
	//alignAddr := (addr + uint64(63)) & (^uint64(63))
	//headLen := alignAddr - addr
	//if headLen != 0 {
	//	panic("only for aligned address")
	//}
	//if l&63 != 0 {
	//	panic("only for aligned length")
	//}
	//
	//if !isAligned(outBitmap, 8) {
	//	panic("only for aligned outBitmap")
	//}
	//bitmapSize := l >> 3
	//if len(outBitmap) < bitmapSize {
	//	panic("outBitmap is too short")
	//}
	bitmapPtr := unsafe.Pointer(unsafe.SliceData(outBitmap))
	unicodeFlagsPtr := unsafe.Pointer(unsafe.SliceData(outUnicodeFlags))
	//
	//blockCnt := (len(b) + 63) / 64
	//if len(outUnicodeFlags) < (blockCnt / 8) {
	//	panic("outUnicodeFlags is too short")
	//}
	//
	for i := 0; i < len(b); i += 64 {
		values := ((*[8]uint64)(unsafe.Add(ptr, i)))
		v := values[0] | values[1] | values[2] | values[3] | values[4] | values[5] | values[6] | values[7]
		v &= asciiMask
		index := (v | -v) >> 63
		table := &lookupTables[index]
		result := uint64(uint64ToMask(values[0], table)) |
			(uint64(uint64ToMask(values[1], table)) << 8) |
			(uint64(uint64ToMask(values[2], table)) << 16) |
			(uint64(uint64ToMask(values[3], table)) << 24) |
			(uint64(uint64ToMask(values[4], table)) << 32) |
			(uint64(uint64ToMask(values[5], table)) << 40) |
			(uint64(uint64ToMask(values[6], table)) << 48) |
			(uint64(uint64ToMask(values[7], table)) << 56)
		// 写回
		//if i == 0 {
		*(*uint64)(unsafe.Add(bitmapPtr, i>>3)) = result
		//}
		//blockIndex := i / 64
		//byteIndex := blockIndex / 8
		//bitIndex := blockIndex % 8
		//outUnicodeFlags[byteIndex] |= (uint8(index) << (bitIndex))

		flags := (*uint8)(unsafe.Add(unicodeFlagsPtr, i>>9))
		if index == 1 {
			// 减少写
			*flags |= (uint8(index) << ((i >> 6) & 7))
		}
		//outUnicodeFlags[i>>9] |= (uint8(index) << ((i >> 6) & 7))
	}
}

func isAligned(arr []byte, n int) bool {
	ptr := unsafe.Pointer(unsafe.SliceData(arr))
	addr := uint64(uintptr(ptr))
	mask := (uint64(n)) - 1
	alignAddr := (addr + mask) & (^mask)
	headLen := alignAddr - addr
	return headLen == 0
}

func getHeadLenForAlign(arr []byte, n int) int {
	ptr := unsafe.Pointer(unsafe.SliceData(arr))
	addr := uint64(uintptr(ptr))
	mask := (uint64(n)) - 1
	alignAddr := (addr + mask) & (^mask)
	headLen := alignAddr - addr
	return int(headLen)
}

//go:linkname MemsetZero runtime.memclrNoHeapPointers
func MemsetZero(ptr unsafe.Pointer, n uintptr)

func SetZero(b []byte) {
	MemsetZero(unsafe.Pointer(unsafe.SliceData(b)), uintptr(len(b)))
}

func stringToBitmapV1(b []byte, outBitmap []byte, outUnicodeFlags []byte) {
	ptr := unsafe.Pointer(unsafe.SliceData(b))
	bitmapPtr := unsafe.Pointer(unsafe.SliceData(outBitmap))
	unicodeFlagsPtr := unsafe.Pointer(unsafe.SliceData(outUnicodeFlags))
	for i := 0; i < len(b); i += 64 {
		values := ((*[8]uint64)(unsafe.Add(ptr, i)))
		v := values[0] | values[1] | values[2] | values[3] | values[4] | values[5] | values[6] | values[7]
		v &= asciiMask
		index := (v | -v) >> 63
		table := &lookupTables[index]
		result := uint64(uint64PtrToMask(unsafe.Add(ptr, i), table)) |
			(uint64(uint64PtrToMask(unsafe.Add(ptr, i+8), table)) << 8) |
			(uint64(uint64PtrToMask(unsafe.Add(ptr, i+16), table)) << 16) |
			(uint64(uint64PtrToMask(unsafe.Add(ptr, i+24), table)) << 24) |
			(uint64(uint64PtrToMask(unsafe.Add(ptr, i+32), table)) << 32) |
			(uint64(uint64PtrToMask(unsafe.Add(ptr, i+40), table)) << 40) |
			(uint64(uint64PtrToMask(unsafe.Add(ptr, i+48), table)) << 48) |
			(uint64(uint64PtrToMask(unsafe.Add(ptr, i+56), table)) << 56)
		// 写回
		*(*uint64)(unsafe.Add(bitmapPtr, i>>3)) = result
		flags := (*uint8)(unsafe.Add(unicodeFlagsPtr, i>>9))
		if index == 1 {
			// 减少写
			*flags |= (uint8(index) << ((i >> 6) & 7))
		}
	}
}

func uint64PtrToMask(ptr unsafe.Pointer, table *[256]byte) uint8 {
	chars := (*[8]uint8)(ptr)
	return (table[chars[0]]) |
		(table[chars[1]] << 1) |
		(table[chars[2]] << 2) |
		(table[chars[3]] << 3) |
		(table[chars[4]] << 4) |
		(table[chars[5]] << 5) |
		(table[chars[6]] << 6) |
		(table[chars[7]] << 7)
}

func stringToBitmapV2(b []byte, outBitmap []byte, outUnicodeFlags []byte) {
	ptr := unsafe.Pointer(unsafe.SliceData(b))
	bitmapPtr := unsafe.Pointer(unsafe.SliceData(outBitmap))
	unicodeFlagsPtr := unsafe.Pointer(unsafe.SliceData(outUnicodeFlags))
	var charsResult [8]uint8
	resultPtr := (*uint64)(unsafe.Pointer(&charsResult))
	for i := 0; i < len(b); i += 64 {
		values := ((*[8]uint64)(unsafe.Add(ptr, i)))
		v := values[0] | values[1] | values[2] | values[3] | values[4] | values[5] | values[6] | values[7]
		v &= asciiMask
		index := (v | -v) >> 63
		table := &lookupTables[index]
		//
		charsResult[0] = uint64ToMask(values[0], table)
		charsResult[1] = uint64ToMask(values[1], table)
		charsResult[2] = uint64ToMask(values[2], table)
		charsResult[3] = uint64ToMask(values[3], table)
		charsResult[4] = uint64ToMask(values[4], table)
		charsResult[5] = uint64ToMask(values[5], table)
		charsResult[6] = uint64ToMask(values[6], table)
		charsResult[7] = uint64ToMask(values[7], table)
		// 写回
		*(*uint64)(unsafe.Add(bitmapPtr, i>>3)) = *resultPtr
		flags := (*uint8)(unsafe.Add(unicodeFlagsPtr, i>>9))
		if index == 1 {
			// 减少写
			*flags |= (uint8(index) << ((i >> 6) & 7))
		}
	}
}
