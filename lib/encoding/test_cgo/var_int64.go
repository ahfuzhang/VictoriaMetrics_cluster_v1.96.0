//go:build !amd64
// +build !amd64

package encoding

// zigzap encoding for int64

/*
#include "var_int64.h"
*/
import "C"
import (
	"unsafe"
)

func MarshalVarInt64sV12(dst []byte, vs []int64) []byte {
	if cap(dst)-len(dst) < len(vs)*10 {
		temp := make([]byte, 0, cap(dst)+len(vs)*10)
		temp = append(temp, dst...)
		dst = temp
	}
	var marshalCount uint64
	C.marshal_var_int64s((*C.SliceHeader)(unsafe.Pointer(&dst)),
		(*C.SliceHeader)(unsafe.Pointer(&vs)),
		(*C.uint64_t)(unsafe.Pointer(&marshalCount)),
	)
	if marshalCount != uint64(len(vs)) {
		panic("not process all")
	}
	return dst
}
