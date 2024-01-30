package encoding

// zigzap encoding for int64

/*
#include "var_int64.h"
*/
import "C"
import (
	"unsafe"

	"github.com/petermattis/fastcgo"
)

func MarshalVarInt64sV12(dst []byte, vs []int64) []byte {
	if cap(dst)-len(dst) < len(vs)*10 {
		temp := make([]byte, 0, cap(dst)+len(vs)*10)
		temp = append(temp, dst...)
		dst = temp
	}
	var marshalCount uint64
	fastcgo.UnsafeCall4(C.marshal_var_int64s,
		uint64(uintptr(unsafe.Pointer(&dst))),
		uint64(uintptr(unsafe.Pointer(&vs))),
		uint64(uintptr(unsafe.Pointer(&marshalCount))),
		0,
	)
	if marshalCount != uint64(len(vs)) {
		panic("not process all")
	}
	return dst
}
