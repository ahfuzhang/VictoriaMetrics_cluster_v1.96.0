package encoding

import "testing"

func Test_IsConst(t *testing.T) {
	var arr = [100]int64{123}
	ret := IsConst(arr[:])
	t.Logf("ret=%+v, %T", ret, ret)
}

func Test_IsConst_2(t *testing.T) {
	var arr = [1024]int64{0}
	ret := IsConst(arr[:])
	if !ret {
		t.Errorf("should be true")
	}
}

func Test_IsConst_3(t *testing.T) {
	var arr = [1024]int64{0}
	arr[len(arr)-1] = 2
	ret := IsConst(arr[:])
	if ret {
		t.Errorf("should be false")
	}
}

/*
go test -timeout 30s -run ^Test_IsConst$ github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding
*/
