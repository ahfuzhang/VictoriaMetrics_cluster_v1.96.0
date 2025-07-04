package logstorage

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"
	"unsafe"
)

func BenchmarkBlock_MustInitFromRows(b *testing.B) {
	for _, rowsPerBlock := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("rowsPerBlock_%d", rowsPerBlock), func(b *testing.B) {
			benchmarkBlockMustInitFromRows(b, rowsPerBlock)
		})
	}
}

func benchmarkBlockMustInitFromRows(b *testing.B, rowsPerBlock int) {
	timestamps, rows := newTestRows(rowsPerBlock, 10)
	b.ReportAllocs()
	b.SetBytes(int64(len(timestamps)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		block := getBlock()
		defer putBlock(block)
		for pb.Next() {
			block.MustInitFromRows(timestamps, rows)
			if n := block.Len(); n != len(timestamps) {
				panic(fmt.Errorf("unexpected block length; got %d; want %d", n, len(timestamps)))
			}
		}
	})
}

// 创建 rowsCount 行日志
func newTestRows(rowsCount, fieldsPerRow int) ([]int64, [][]Field) {
	timestamps := make([]int64, rowsCount)
	rows := make([][]Field, rowsCount)
	for i := range timestamps {
		timestamps[i] = int64(i) * 1e9
		fields := make([]Field, fieldsPerRow)
		for j := range fields {
			f := &fields[j]
			f.Name = fmt.Sprintf("field_%d", j)        // 表格形状的
			f.Value = fmt.Sprintf("value_%d_%d", i, j) // tag value 的内容全都不同
		}
		rows[i] = fields
	}
	return timestamps, rows
}

// --------------------------------------------------------------------------------

type testData struct {
	timestamps []int64
	rows       [][]Field
	b          []byte // arena
}

const bytesPerCol int = 50

// 产生一个列名
func (td *testData) genColName(colIndex int) string {
	ptr := unsafe.Pointer(unsafe.SliceData(td.b))
	start := len(td.b)
	td.b = fmt.Appendf(td.b, "col_%05d", colIndex)
	end := len(td.b)
	colName := unsafe.String((*byte)(unsafe.Add(ptr, start)), end-start)
	return colName
}

func (td *testData) copyString(s string) string {
	ptr := unsafe.Pointer(unsafe.SliceData(td.b))
	start := len(td.b)
	td.b = append(td.b, s...)
	end := len(td.b)
	colName := unsafe.String((*byte)(unsafe.Add(ptr, start)), end-start)
	return colName
}

// func (td *testData) genColNameDiffLen(colIndex int) string {
// 	ptr := unsafe.Pointer(unsafe.SliceData(td.b))
// 	start := len(td.b)
// 	td.b = fmt.Appendf(td.b, "col_%d", colIndex)
// 	end := len(td.b)
// 	colName := unsafe.String((*byte)(unsafe.Add(ptr, start)), end-start)
// 	return colName
// }

func (td *testData) genColNames(cnt int) []string {
	colNames := make([]string, 0, cnt)
	for i := 0; i < cnt; i++ {
		s := fmt.Sprintf("col_%d", i+200+rand.IntN(1000000))
		colNames = append(colNames, s)
	}
	return colNames
}

// 产生一个 tag value
func (td *testData) genColValue(length int) string {
	ptr := unsafe.Pointer(unsafe.SliceData(td.b))
	start := len(td.b)
	for i := 0; i < length; i++ {
		td.b = append(td.b, byte(rand.IntN(126-32)+32))
	}
	end := len(td.b)
	colValue := unsafe.String((*byte)(unsafe.Add(ptr, start)), end-start)
	return colValue
}

func (td *testData) genConstColValue(s string) string {
	ptr := unsafe.Pointer(unsafe.SliceData(td.b))
	start := len(td.b)
	td.b = append(td.b, s...)
	end := len(td.b)
	colValue := unsafe.String((*byte)(unsafe.Add(ptr, start)), end-start)
	return colValue
}

func (td *testData) genTimestamps(rowsCount int) {
	td.timestamps = make([]int64, rowsCount)
	for i := 0; i < rowsCount; i++ {
		td.timestamps[i] = int64(i) + 1e9
	}
}

// 产生 n 行 m 列
func (td *testData) Gen(rowsCount int, colCount int) {
	td.genTimestamps(rowsCount)
	td.b = make([]byte, 0, colCount*bytesPerCol*rowsCount*2)
	td.rows = make([][]Field, 0, rowsCount)
	for i := 0; i < rowsCount; i++ {
		row := make([]Field, 0, colCount)
		for j := 0; j < colCount; j++ {
			if cap(td.b)-len(td.b) < bytesPerCol {
				goto endGen
			}
			row = append(row, Field{td.genColName(j), td.genColValue(bytesPerCol - 9 - 3)})
		}
		// 生成 const 列
		row = append(row, Field{td.genColName(colCount + 1), td.genConstColValue("this is const column")})
		//
		rand.Shuffle(len(row), func(i, j int) {
			row[i], row[j] = row[j], row[i]
		})
		td.rows = append(td.rows, row)
	}
endGen:
	if len(td.rows) != len(td.timestamps) {
		panic("not same")
	}

	colCount++
}

// 产生 n 行 m 列
// 列中的 tag value 的长度不是固定的
func (td *testData) Gen2(rowsCount int, colCount int) {
	td.genTimestamps(rowsCount)
	td.b = make([]byte, 0, colCount*bytesPerCol*rowsCount*2)
	td.rows = make([][]Field, 0, rowsCount)
	for i := 0; i < rowsCount; i++ {
		row := make([]Field, 0, colCount)
		for j := 0; j < colCount; j++ {
			if cap(td.b)-len(td.b) < bytesPerCol {
				goto endGen2
			}
			row = append(row, Field{td.genColName(j), td.genColValue(rand.IntN(bytesPerCol - 12))})
		}
		// 生成 const 列
		row = append(row, Field{td.genColName(colCount + 1), td.genConstColValue("this is const column")})
		//
		rand.Shuffle(len(row), func(i, j int) {
			row[i], row[j] = row[j], row[i]
		})
		td.rows = append(td.rows, row)
	}
endGen2:
	if len(td.rows) != len(td.timestamps) {
		panic("not same")
	}
}

func (td *testData) GenDiffNameCol(rowsCount int, colCount int) {
	td.genTimestamps(rowsCount)
	td.b = make([]byte, 0, colCount*bytesPerCol*rowsCount*2)
	td.rows = make([][]Field, 0, rowsCount)
	colNames := td.genColNames(1500)
	for i := 0; i < rowsCount; i++ {
		row := make([]Field, 0, colCount)
		for j := 0; j < colCount; j++ {
			if cap(td.b)-len(td.b) < bytesPerCol {
				goto endGen2
			}
			colName := colNames[rand.IntN(len(colNames))]
			row = append(row, Field{td.copyString(colName), td.genColValue(rand.IntN(bytesPerCol - 12))})
		}
		// 生成 const 列
		row = append(row, Field{td.genColName(colCount + 1), td.genConstColValue("this is const column")})
		//
		rand.Shuffle(len(row), func(i, j int) {
			row[i], row[j] = row[j], row[i]
		})
		td.rows = append(td.rows, row)
	}
endGen2:
	if len(td.rows) != len(td.timestamps) {
		panic("not same")
	}
}

func (td *testData) GenRandomColCount(rowsCount int, colCount int) {
	td.genTimestamps(rowsCount)
	td.b = make([]byte, 0, colCount*bytesPerCol*rowsCount*2)
	td.rows = make([][]Field, 0, rowsCount)
	for i := 0; i < rowsCount; i++ {
		row := make([]Field, 0, colCount)
		newColCount := rand.IntN(colCount-1) + 1
		for j := 0; j < newColCount; j++ {
			if cap(td.b)-len(td.b) < bytesPerCol {
				goto endGenRandomColCount
			}
			row = append(row, Field{td.genColName(j), td.genColValue(rand.IntN(bytesPerCol - 12))})
			//rand.Seed(time.Now().UnixNano())

		}
		// 生成 const 列
		row = append(row, Field{td.genColName(colCount + 1), td.genConstColValue("this is const column")})
		//
		rand.Shuffle(len(row), func(i, j int) {
			row[i], row[j] = row[j], row[i]
		})
		td.rows = append(td.rows, row)
	}
endGenRandomColCount:
	if len(td.rows) != len(td.timestamps) {
		panic("not same")
	}
}

func (td *testData) BytesSize() int {
	return len(td.timestamps)*8 + len(td.b)
}

func runBench(td *testData, b *testing.B) {
	block := getBlock()
	defer putBlock(block)
	b.ReportAllocs()
	b.SetBytes(int64(td.BytesSize()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		block.MustInitFromRows(td.timestamps, td.rows)
		if n := block.Len(); n != len(td.timestamps) {
			panic(fmt.Errorf("unexpected block length; got %d; want %d", n, len(td.timestamps)))
		}
	}
}

// func init() {
// 	runtime.SetCPUProfileRate(1000) // 提高到 1000Hz（1ms 采样一次）
// }

/*
GOMAXPROCS=1 go test -benchmem -run=^$ -bench ^BenchmarkBlock_MustInitFromRowsV2$ github.com/VictoriaMetrics/VictoriaMetrics/lib/logstorage
*/
func BenchmarkBlock_MustInitFromRowsV2(b *testing.B) {
	runtime.SetCPUProfileRate(10000)
	b.Run("same_column_count", func(b *testing.B) {
		td := &testData{}
		td.Gen(1000, 100) // 1000 行日志，每行 100 个字段
		runBench(td, b)
	})
	b.Run("same_column_count_random_value_len", func(b *testing.B) {
		td := &testData{}
		td.Gen(1000, 100) // 1000 行日志，每行 100 个字段
		runBench(td, b)
	})
	b.Run("random_column_count", func(b *testing.B) {
		td := &testData{}
		td.GenRandomColCount(1000, 100) // 1000 行日志，每行 100 个字段
		runBench(td, b)
	})
	b.Run("diff_col_name", func(b *testing.B) {
		td := &testData{}
		td.GenDiffNameCol(1000, 100) // 1000 行日志，每行 100 个字段
		runBench(td, b)
	})
}

/*
GOMAXPROCS=1 go test -benchmem -run=^$ -cpuprofile=cpu.out -bench ^BenchmarkBlock_MustInitFromRowsV3$ github.com/VictoriaMetrics/VictoriaMetrics/lib/logstorage
go tool pprof -http=:8080 cpu.out
*/
func BenchmarkBlock_MustInitFromRowsV3(b *testing.B) {
	b.Run("diff_col_name", func(b *testing.B) {
		td := &testData{}
		td.GenDiffNameCol(1000, 100) // 1000 行日志，每行 100 个字段
		runBench(td, b)
	})
}

// dlv test github.com/VictoriaMetrics/VictoriaMetrics/lib/logstorage -- -test.run ^Test_block_v2$
/*
go test -timeout 30s -run ^Test_block_v2$ -cover -coverprofile=cover.out github.com/VictoriaMetrics/VictoriaMetrics/lib/logstorage
go tool cover -html=cover.out
*/
func Test_block_v2(t *testing.T) {
	f := func(td *testData) {
		block := getBlock()
		defer putBlock(block)
		block.MustInitFromRows(td.timestamps, td.rows)
		if n := block.Len(); n != len(td.timestamps) {
			t.Errorf("unexpected block length; got %d; want %d", n, len(td.timestamps))
		}
	}
	{
		td := &testData{}
		td.Gen(1000, 100)
		f(td)
	}
	{
		td := &testData{}
		td.Gen2(1000, 100)
		f(td)
	}
	{
		td := &testData{}
		td.GenRandomColCount(1000, 100)
		f(td)
	}
}
