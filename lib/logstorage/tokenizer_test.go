package logstorage

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

func TestTokenizeStrings(t *testing.T) {
	f := func(a, tokensExpected []string) {
		t.Helper()
		tokens := tokenizeStrings(nil, a)
		if !reflect.DeepEqual(tokens, tokensExpected) {
			t.Fatalf("unexpected tokens;\ngot\n%q\nwant\n%q", tokens, tokensExpected)
		}
	}
	f(nil, nil)
	f([]string{""}, nil)
	f([]string{"foo"}, []string{"foo"})
	f([]string{"foo bar---.!!([baz]!!! %$# TaSte"}, []string{"foo", "bar", "baz", "TaSte"})
	f([]string{"теСТ 1234 f12.34", "34 f12 AS"}, []string{"теСТ", "1234", "f12", "34", "AS"})
	f(strings.Split(`
Apr 28 13:43:38 localhost whoopsie[2812]: [13:43:38] online
Apr 28 13:45:01 localhost CRON[12181]: (root) CMD (command -v debian-sa1 > /dev/null && debian-sa1 1 1)
Apr 28 13:48:01 localhost kernel: [36020.497806] CPU0: Core temperature above threshold, cpu clock throttled (total events = 22034)
`, "\n"), []string{"Apr", "28", "13", "43", "38", "localhost", "whoopsie", "2812", "online", "45", "01", "CRON", "12181",
		"root", "CMD", "command", "v", "debian", "sa1", "dev", "null", "1", "48", "kernel", "36020", "497806", "CPU0", "Core",
		"temperature", "above", "threshold", "cpu", "clock", "throttled", "total", "events", "22034"})
}

// go test -benchmem -v -run=^$ -bench ^Benchmark_tokenize$ github.com/VictoriaMetrics/VictoriaMetrics/lib/logstorage
// go test -benchmem -v -run=^$ -benchmem -cpuprofile cpu.prof -bench ^Benchmark_tokenize$ github.com/VictoriaMetrics/VictoriaMetrics/lib/logstorage
// go test -bench ^BenchmarkIsASCII$ -benchmem -cpuprofile cpu.prof string_utils
/*
goos: linux
goarch: amd64
pkg: github.com/VictoriaMetrics/VictoriaMetrics/lib/logstorage
cpu: Intel(R) Xeon(R) Platinum 8260 CPU @ 2.40GHz
Benchmark_tokenize
Benchmark_tokenize-8          27          37706764 ns/op          52.48 MB/s     3524736 B/op         38 allocs/op
*/
func Benchmark_tokenize(b *testing.B) {
	data, err := os.ReadFile("../../tests/data/rfc-ref.txt")
	if err != nil {
		b.Error(err)
		return
	}
	s := unsafe.String(&data[0], len(data))
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tokenizeStrings(nil, []string{s})
	}
}
