package logstorage

import (
	"strings"
	"testing"
)

// 3392.69 MB/s
func BenchmarkTokenizeHashes(b *testing.B) {
	a := strings.Split(benchLogs, "\n")

	b.ReportAllocs()
	b.SetBytes(int64(len(benchLogs)))
	b.RunParallel(func(pb *testing.PB) {
		var hashes []uint64
		for pb.Next() {
			hashes = tokenizeHashes(hashes[:0], a)
		}
	})
}
