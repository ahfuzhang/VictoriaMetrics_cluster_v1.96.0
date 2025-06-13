package stringsutil

import (
	"sort"
	"sync"
)

// Values Batch processing of multiple strings
type Values struct {
	values []string
	bitmap []byte
	b      []byte
}

var valuesPool = sync.Pool{
	New: func() interface{} {
		return &Values{}
	},
}

// GetValues returns a Values from the pool
func GetValues() *Values {
	return valuesPool.Get().(*Values)
}

// PutValues puts a Values to the pool
func PutValues(v *Values) {
	valuesPool.Put(v)
}

// Reset resets the Values
func (vs *Values) Reset() {
	vs.values = vs.values[:0]
	vs.bitmap = vs.bitmap[:0]
	vs.b = vs.b[:0]
}

// Init initializes the Values
func (vs *Values) Init(values []string) {
	vs.Reset()
	vs.values = append(vs.values, values...)
	if !sort.StringsAreSorted(vs.values) {
		sort.Strings(vs.values)
	}
	totalLength := 0
	for _, s := range vs.values {
		totalLength += len(s)
	}
	bitmapLen := (len(vs.values) + 7) / 8
	if cap(vs.bitmap) < bitmapLen {
		vs.bitmap = make([]byte, 0, bitmapLen)
	}
	vs.bitmap = vs.bitmap[:bitmapLen]
	for i := range vs.bitmap {
		vs.bitmap[i] = 0
	}
	if cap(vs.b) < totalLength+len(vs.values) {
		vs.b = make([]byte, 0, totalLength)
	}
	for i := range vs.values {
		if i > 0 && vs.values[i] == vs.values[i-1] {
			vs.bitmap[i/8] |= (1 << (i & 7)) // set duplicate flag
			continue
		}
		vs.b = append(vs.b, vs.values[i]...)
		vs.b = append(vs.b, 0)
	}
}
