package logstorage

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/slicesutil"
)

// block represents a block of log entries.
type block struct {
	// timestamps contains timestamps for log entries.
	timestamps []int64 // 所有的时间戳，数组长度等于日志条数

	// columns contains values for fields seen in log entries.  // tag name -> values
	columns []column // 一个 name, 多个 value  // 按照名字排序

	// constColumns contains fields with constant values across all the block entries.
	constColumns []Field //  tag name -> tag value  // 按照名字排序
	// 所有的 tag value 都相同的时候，这个列是 const 列
}

func (b *block) reset() {
	b.timestamps = b.timestamps[:0]

	cs := b.columns
	for i := range cs {
		cs[i].reset()
	}
	b.columns = cs[:0]

	ccs := b.constColumns
	for i := range ccs {
		ccs[i].Reset()
	}
	b.constColumns = ccs[:0]
}

// uncompressedSizeBytes returns the total size of the original log entries stored in b.
//
// It is supposed that every log entry has the following format:
//
// 2006-01-02T15:04:05.999999999Z07:00 field1=value1 ... fieldN=valueN
func (b *block) uncompressedSizeBytes() uint64 { // 计算未压缩的数据长度
	rowsCount := uint64(b.Len())

	// Take into account timestamps
	n := rowsCount * uint64(len(time.RFC3339Nano)) // 水分很大啊，这也算长度 ?

	// Take into account columns
	cs := b.columns //??? 为什么要这么写呢？ 难道可以提升性能吗?
	for i := range cs {
		c := &cs[i]
		nameLen := uint64(len(c.name))
		if nameLen == 0 {
			nameLen = uint64(len("_msg"))
		}
		for _, v := range c.values {
			if len(v) > 0 {
				n += nameLen + 2 + uint64(len(v))
			}
		}
	}

	// Take into account constColumns
	ccs := b.constColumns
	for i := range ccs {
		cc := &ccs[i]
		nameLen := uint64(len(cc.Name))
		if nameLen == 0 {
			nameLen = uint64(len("_msg"))
		}
		n += rowsCount * (2 + nameLen + uint64(len(cc.Value)))
	}

	return n
}

// uncompressedRowsSizeBytes returns the size of the uncompressed rows.
//
// It is supposed that every row has the following format:
//
// 2006-01-02T15:04:05.999999999Z07:00 field1=value1 ... fieldN=valueN
func uncompressedRowsSizeBytes(rows [][]Field) uint64 {
	n := uint64(0)
	for _, fields := range rows {
		n += uncompressedRowSizeBytes(fields)
	}
	return n
}

// uncompressedRowSizeBytes returns the size of uncompressed row.
//
// It is supposed that the row has the following format:
//
// 2006-01-02T15:04:05.999999999Z07:00 field1=value1 ... fieldN=valueN
func uncompressedRowSizeBytes(fields []Field) uint64 {
	n := uint64(len(time.RFC3339Nano)) // log timestamp
	for _, f := range fields {
		nameLen := len(f.Name)
		if nameLen == 0 {
			nameLen = len("_msg")
		}
		n += uint64(2 + nameLen + len(f.Value))
	}
	return n
}

// column contains values for the given field name seen in log entries.
type column struct {
	// name is the field name  // name 为空，则是 _msg
	name string

	// values is the values seen for the given log entries.
	values []string
}

func (c *column) reset() {
	c.name = ""

	clear(c.values)
	c.values = c.values[:0]
}

func (c *column) canStoreInConstColumn() bool {
	values := c.values
	if len(values) == 0 {
		return true
	}
	value := values[0]
	if len(value) > maxConstColumnValueSize {
		return false
	}
	for _, v := range values[1:] { // 所有的 value 都一样，那么就是 const column
		if len(value) != len(v) {
			return false
		}
	}
	for _, v := range values[1:] { // 所有的 value 都一样，那么就是 const column
		if value != v {
			return false
		}
	}
	return true
}

func (c *column) resizeValues(valuesLen int) []string {
	c.values = slicesutil.SetLength(c.values, valuesLen)
	return c.values
}

// mustWriteTo writes c to sw and updates ch accordingly.  // 看起来是做序列化
//
// ch is valid until c is changed.
func (c *column) mustWriteTo(ch *columnHeader, sw *streamWriters) {
	ch.reset()

	ch.name = c.name

	bloomValuesWriter := sw.getBloomValuesWriterForColumnName(ch.name) // 看起来是 bloom filter 的构造器

	// encode values
	ve := getValuesEncoder()
	// todo: 如果这里对 c.values 进行排序和拷贝，是否会有收益???
	// 判断是否是相同的数据类型
	ch.valueType, ch.minValue, ch.maxValue = ve.encode(c.values, &ch.valuesDict) //todo: 这里尝试了多种序列化算法. 这里消耗 1.6%

	bb := longTermBufPool.Get()
	defer longTermBufPool.Put(bb)

	// marshal values
	// ve.values, 这里已经根据数据类型来序列化了
	bb.B = marshalStringsBlock(bb.B[:0], ve.values) // 序列化，并使用 zstd 压缩。消耗写入的 15%，不好优化
	putValuesEncoder(ve)
	ch.valuesSize = uint64(len(bb.B))
	if ch.valuesSize > maxValuesBlockSize {
		logger.Panicf("BUG: too valuesSize: %d bytes; mustn't exceed %d bytes", ch.valuesSize, maxValuesBlockSize)
	}
	ch.valuesOffset = bloomValuesWriter.values.bytesWritten
	bloomValuesWriter.values.MustWrite(bb.B) // 写入文件， values 部分

	// create and marshal bloom filter for c.values
	if ch.valueType != valueTypeDict {
		hashesBuf := encoding.GetUint64s(0)
		// ??? c.values 是排序好的吗? => 从之前的代码看 c.values 是未排序的
		hashesBuf.A = tokenizeHashes(hashesBuf.A[:0], c.values) //todo: 这里消耗了 16% 的 cpu 资源
		// 计算出 n 个 hash 值，然后写入文件
		bb.B = bloomFilterMarshalHashes(bb.B[:0], hashesBuf.A) // block 中序列化数据, 消耗写入的 11%
		encoding.PutUint64s(hashesBuf)
	} else {
		// there is no need in ecoding bloom filter for dictionary type,
		// since it isn't used during querying - all the dictionary values are available in ch.valuesDict
		bb.B = bb.B[:0]
	}
	ch.bloomFilterSize = uint64(len(bb.B))
	if ch.bloomFilterSize > maxBloomFilterBlockSize {
		logger.Panicf("BUG: too big bloomFilterSize: %d bytes; mustn't exceed %d bytes", ch.bloomFilterSize, maxBloomFilterBlockSize)
	}
	ch.bloomFilterOffset = bloomValuesWriter.bloom.bytesWritten
	bloomValuesWriter.bloom.MustWrite(bb.B)
}

func (b *block) assertValid() {
	// Check that timestamps are in ascending order
	timestamps := b.timestamps
	for i := 1; i < len(timestamps); i++ {
		// todo: 使用 simd 来优化
		if timestamps[i-1] > timestamps[i] {
			logger.Panicf("BUG: log entries must be sorted by timestamp; got the previous entry with bigger timestamp %d than the current entry with timestamp %d",
				timestamps[i-1], timestamps[i])
		}
	}

	// Check that the number of items in each column matches the number of items in the block.
	itemsCount := len(timestamps)
	columns := b.columns
	for _, c := range columns { // column 中 value 的数量，与行的数量一致
		if len(c.values) != itemsCount {
			logger.Panicf("BUG: unexpected number of values for column %q: got %d; want %d", c.name, len(c.values), itemsCount)
		}
	}
}

// MustInitFromRows initializes b from the given timestamps and rows.
//
// It is expected that timestamps are sorted.
//
// b is valid until rows are changed.
func (b *block) MustInitFromRows(timestamps []int64, rows [][]Field) { // 把多行日志，变成 block 对象
	b.reset()
	// 此时 rows 已经排序好了
	assertTimestampsSorted(timestamps)   // todo: 可以 simd 优化
	b.mustInitFromRows(timestamps, rows) // 区分普通列和 const 列
	b.sortColumnsByName()                // 按照 tag name 排序
}

/*
普通列： tag_name = [value1, value2..., value_n]
const 列： tag_name = tag value
*/

// mustInitFromRows initializes b from the given timestamps and rows.
//
// b is valid until rows are changed.
// @param rows 已经排序好了
// 这个函数消耗 17% 的 cpu 写入 cpu
// 在 block 的 benchmark 中，这个函数消耗 84%
func (b *block) mustInitFromRows(timestamps []int64, rows [][]Field) { // 把多行日志，变成 block 对象
	if len(timestamps) != len(rows) {
		logger.Panicf("BUG: len of timestamps %d and rows %d must be equal", len(timestamps), len(rows))
	}

	rowsLen := len(rows)
	if rowsLen == 0 {
		// Nothing to do
		return
	}
	// areSameFieldsInRows(rows) 为 true， 说明 rows 是个标准的二维表格
	if areSameFieldsInRows(rows) {
		// Fast path - all the log entries have the same fields
		b.timestamps = append(b.timestamps, timestamps...)
		fields := rows[0]
		for i := range fields { // 遍历 tag name
			f := &fields[i]
			if canStoreInConstColumn(rows, i) { // 是不是这一列的所有值都一样
				cc := b.extendConstColumns() // 产生一个新的 const 列
				cc.Name = f.Name
				cc.Value = f.Value // const 列的值，存一份就够了
			} else {
				c := b.extendColumns() // 增加一个新的普通列
				c.name = f.Name
				values := c.resizeValues(rowsLen)
				_ = values[len(rows)-1]  // 越界检查
				for j := range rows {
					v := rows[j][i].Value  // 越界检查
					values[j] = v // 这里是存储了所有的 tag value 吗？
					//todo: 优化点，如果与前一列的值相同，则可以精简
					// 注意：这里的 values 不是有序的
				}
			}
		}
		return
	}

	// Slow path - log entries contain different set of fields

	// Determine indexes for columns

	columnIdxs := getColumnIdxs()
	i := 0
	for i < len(rows) {
		fields := rows[i]
		if len(columnIdxs)+len(fields) > maxColumnsPerBlock { // 最多允许 2000 个 tagName
			// todo: 这里应该做成可配置的
			// User tries writing too many unique field names into a single log stream.
			// It is better ignoring rows with too many field names instead of trying to store them,
			// since the storage isn't designed to work with too big number of unique field names
			// per log stream - this leads to excess usage of RAM, CPU, disk IO and disk space.
			// It is better emitting a warning, so the user is aware of the problem and fixes it ASAP.
			fieldNames := make([]string, 0, len(columnIdxs))
			for k := range columnIdxs {
				fieldNames = append(fieldNames, k)
			}
			// todo: 这里应该 metrics 上报
			logger.Warnf("ignoring %d rows in the block, because they contain more than %d unique field names: %s", len(rows)-i, maxColumnsPerBlock, fieldNames)
			break
		}
		for j := range fields {
			name := fields[j].Name
			if _, ok := columnIdxs[name]; !ok {
				columnIdxs[name] = len(columnIdxs) // 避免名字重复的 tag name
			}
		}
		i++
	}
	rowsProcessed := i

	// keep only rows that fit maxColumnsPerBlock limit
	rows = rows[:rowsProcessed] // 总字段数超过 2000 后，后面的行会被丢弃  // 越界检查
	// todo: 丢弃的行应该加上 metrics 上报
	timestamps = timestamps[:rowsProcessed]// 越界检查
	if len(rows) == 0 {
		return
	}

	b.timestamps = append(b.timestamps, timestamps...)

	// Initialize columns
	cs := b.resizeColumns(len(columnIdxs))
	_ = cs[len(columnIdxs)-1]  // 越界检查
	for name, idx := range columnIdxs {
		c := &cs[idx]  // 越界检查
		c.name = name // 列名
		c.resizeValues(len(rows))
	}

	// Write rows to block
	for i := range rows {
		for _, f := range rows[i] { // 遍历行和列
			idx := columnIdxs[f.Name]
			cs[idx].values[i] = f.Value // 从这里看， values 并未排序  // 越界检查, 两次
		}
	}
	putColumnIdxs(columnIdxs)

	// Detect const columns
	for i := len(cs) - 1; i >= 0; i-- {
		c := &cs[i]
		if !c.canStoreInConstColumn() { // 是否所有的 value 都一样
			continue
		}
		cc := b.extendConstColumns()
		cc.Name = c.name
		cc.Value = c.values[0]

		c.reset() // 这一列是 const 列，则从 columns 数组里删除掉
		if i < len(cs)-1 {
			swapColumns(c, &cs[len(cs)-1])
		}
		cs = cs[:len(cs)-1]
	}
	b.columns = cs
}

func swapColumns(a, b *column) {
	*a, *b = *b, *a
}

func canStoreInConstColumn(rows [][]Field, colIdx int) bool { // 检查第 n 列
	if len(rows) == 0 {
		return true
	}
	value := rows[0][colIdx].Value
	if len(value) > maxConstColumnValueSize {
		return false
	}
	rows = rows[1:]
	for i := range rows {
		// todo: 先比较一轮长度
		//todo: 去掉数组下标检查
		if len(value) != len(rows[i][colIdx].Value) { // 是不是这一列的所有值都一样？
			return false
		}
	}
	for i := range rows {
		// todo: 先比较一轮长度
		//todo: 去掉数组下标检查
		if value != rows[i][colIdx].Value { // 是不是这一列的所有值都一样？
			return false
		}
	}
	return true
}

/*
可以使用 simd 来优化
#include <immintrin.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

	bool is_sorted_avx2(const int64_t *arr, size_t n) {
	    size_t i = 0;
	    size_t limit = n > 4 ? n - 4 : 0;

	    // 主循环：一次比较4对
	    for (; i < limit; i += 4) {
	        __m256i current = _mm256_loadu_si256((__m256i const *)(arr + i));
	        __m256i next = _mm256_loadu_si256((__m256i const *)(arr + i + 1));

	        __m256i cmp = _mm256_cmpgt_epi64(current, next);
	        if (_mm256_movemask_epi8(cmp)) {
	            return false;
	        }
	    }

	    // 处理剩余的
	    for (; i < n - 1; i++) {
	        if (arr[i] > arr[i + 1]) {
	            return false;
	        }
	    }

	    return true;
	}
*/
func assertTimestampsSorted(timestamps []int64) {
	// todo: 这里应该使用 simd 来优化
	for i := range timestamps {
		if i > 0 && timestamps[i-1] > timestamps[i] {
			logger.Panicf("BUG: log entries must be sorted by timestamp; got the previous entry with bigger timestamp %d than the current entry with timestamp %d",
				timestamps[i-1], timestamps[i])
		}
	}
}

func (b *block) extendConstColumns() *Field {
	ccs := b.constColumns
	if cap(ccs) > len(ccs) {
		ccs = ccs[:len(ccs)+1]
	} else {
		ccs = append(ccs, Field{})
	}
	b.constColumns = ccs
	return &ccs[len(ccs)-1]
}

func (b *block) extendColumns() *column {
	cs := b.columns
	if cap(cs) > len(cs) {
		cs = cs[:len(cs)+1]
	} else {
		cs = append(cs, column{})
	}
	b.columns = cs
	return &cs[len(cs)-1]
}

func (b *block) resizeColumns(columnsLen int) []column {
	b.columns = slicesutil.SetLength(b.columns, columnsLen)
	return b.columns
}

func (b *block) sortColumnsByName() {
	if len(b.columns)+len(b.constColumns) > maxColumnsPerBlock {
		columnNames := b.getColumnNames()
		logger.Panicf("BUG: too big number of columns detected in the block: %d; the number of columns mustn't exceed %d; columns: %s",
			len(b.columns)+len(b.constColumns), maxColumnsPerBlock, columnNames)
	}

	cs := getColumnsSorter()
	cs.columns = b.columns
	sort.Sort(cs) // 对列排序, 按照 tag name 排序
	putColumnsSorter(cs)

	ccs := getConstColumnsSorter()
	ccs.columns = b.constColumns
	sort.Sort(ccs) // 也是按照 tag name 排序
	putConstColumnsSorter(ccs)
}

func (b *block) getColumnNames() []string {
	a := make([]string, 0, len(b.columns)+len(b.constColumns))
	for _, c := range b.columns {
		a = append(a, c.name)
	}
	for _, c := range b.constColumns {
		a = append(a, c.Name)
	}
	return a
}

// Len returns the number of log entries in b.
func (b *block) Len() int {
	return len(b.timestamps) // block 中的记录数
}

// InitFromBlockData unmarshals bd to b.
//
// sbu and vd are used as a temporary storage for unmarshaled column values.
//
// The b becomes outdated after sbu or vd is reset.
func (b *block) InitFromBlockData(bd *blockData, sbu *stringsBlockUnmarshaler, vd *valuesDecoder) error {
	b.reset()

	if bd.rowsCount > maxRowsPerBlock {
		return fmt.Errorf("too many entries found in the block: %d; mustn't exceed %d", bd.rowsCount, maxRowsPerBlock)
	}
	rowsCount := int(bd.rowsCount)

	// unmarshal timestamps
	td := &bd.timestampsData
	var err error
	b.timestamps, err = encoding.UnmarshalTimestamps(b.timestamps[:0], td.data, td.marshalType, td.minTimestamp, rowsCount)
	if err != nil {
		return fmt.Errorf("cannot unmarshal timestamps: %w", err)
	}

	// unmarshal columns
	cds := bd.columnsData
	cs := b.resizeColumns(len(cds))
	for i := range cds {
		cd := &cds[i]
		c := &cs[i]
		c.name = sbu.copyString(cd.name)
		c.values, err = sbu.unmarshal(c.values[:0], cd.valuesData, uint64(rowsCount))
		if err != nil {
			return fmt.Errorf("cannot unmarshal column %d: %w", i, err)
		}
		if err = vd.decodeInplace(c.values, cd.valueType, cd.valuesDict.values); err != nil {
			return fmt.Errorf("cannot decode column values: %w", err)
		}
	}

	// unmarshal constColumns
	b.constColumns = sbu.appendFields(b.constColumns[:0], bd.constColumns)

	return nil
}

// mustWriteTo writes b with the given sid to sw and updates bh accordingly.
func (b *block) mustWriteTo(sid *streamID, bh *blockHeader, sw *streamWriters) {
	b.assertValid() // todo: simd 优化
	bh.reset()

	bh.streamID = *sid
	bh.uncompressedSizeBytes = b.uncompressedSizeBytes() // 计算未压缩的数据大小
	bh.rowsCount = uint64(b.Len())

	// Marshal timestamps
	mustWriteTimestampsTo(&bh.timestampsHeader, b.timestamps, sw) // 序列化时间戳部分

	// Marshal columns

	csh := getColumnsHeader()

	cs := b.columns
	chs := csh.resizeColumnHeaders(len(cs)) // 列头对象
	for i := range cs {
		cs[i].mustWriteTo(&chs[i], sw) // 写入每一列
	}

	csh.constColumns = append(csh.constColumns[:0], b.constColumns...) // 写入 const 列

	csh.mustWriteTo(bh, sw)

	putColumnsHeader(csh)
}

// appendRowsTo appends log entries from b to dst.
func (b *block) appendRowsTo(dst *rows) {
	// copy timestamps
	dst.timestamps = append(dst.timestamps, b.timestamps...)

	// copy columns
	ccs := b.constColumns
	cs := b.columns

	// Pre-allocate dst.fieldsBuf for all the fields across rows.
	fieldsCount := len(b.timestamps) * (len(ccs) + len(cs))
	fieldsBuf := slicesutil.SetLength(dst.fieldsBuf, len(dst.fieldsBuf)+fieldsCount)
	fieldsBuf = fieldsBuf[:len(fieldsBuf)-fieldsCount]

	// Pre-allocate dst.rows
	dst.rows = slicesutil.SetLength(dst.rows, len(dst.rows)+len(b.timestamps))
	dstRows := dst.rows[len(dst.rows)-len(b.timestamps):]

	for i := range b.timestamps {
		fieldsLen := len(fieldsBuf)
		// copy const columns
		fieldsBuf = append(fieldsBuf, ccs...)
		// copy other columns
		for j := range cs {
			c := &cs[j]
			value := c.values[i]
			if len(value) == 0 {
				continue
			}
			fieldsBuf = append(fieldsBuf, Field{
				Name:  c.name,
				Value: value,
			})
		}
		dstRows[i] = fieldsBuf[fieldsLen:]
	}
	dst.fieldsBuf = fieldsBuf
}

// 在 block 的 benchmark 中，消耗 50% cpu
func areSameFieldsInRows(rows [][]Field) bool { // 是否是 tag 完全相同的多个 row
	if len(rows) < 2 {
		return true
	}
	fields := rows[0]

	// Verify that all the field names are unique  // ??? 为什么只检查第 0 行
	m := getFieldsSet()
	for i := range fields { // 确保 tag name 都是唯一的
		f := &fields[i]
		if _, ok := m[f.Name]; ok {
			// Field name isn't unique
			return false
		}
		m[f.Name] = struct{}{} // 按照 tag name 建立索引
	}
	putFieldsSet(m)

	// Verify that all the fields are the same across rows
	rows = rows[1:]
	for i := range rows { // todo: 可以通过 simd 来快速比较所有的 string 的长度
		leFields := rows[i]
		if len(fields) != len(leFields) { // 每一行都和第 0 行比较
			return false
		}
		for j := range leFields {
			// todo: 先快速比较长度
			if len(leFields[j].Name) != len(fields[j].Name) {
				return false
			}
		}
		for j := range leFields {
			// todo: 先快速比较长度
			if leFields[j].Name != fields[j].Name {
				return false
			}
		}
	}
	return true
}

func getFieldsSet() map[string]struct{} {
	v := fieldsSetPool.Get()
	if v == nil {
		return make(map[string]struct{})
	}
	return v.(map[string]struct{})
}

func putFieldsSet(m map[string]struct{}) {
	clear(m)
	fieldsSetPool.Put(m)
}

var fieldsSetPool sync.Pool

var columnIdxsPool sync.Pool

func getColumnIdxs() map[string]int {
	v := columnIdxsPool.Get()
	if v == nil {
		return make(map[string]int)
	}
	return v.(map[string]int)
}

func putColumnIdxs(m map[string]int) {
	clear(m)
	columnIdxsPool.Put(m)
}

func getBlock() *block {
	v := blockPool.Get()
	if v == nil {
		return &block{}
	}
	return v.(*block)
}

func putBlock(b *block) {
	b.reset()
	blockPool.Put(b)
}

var blockPool sync.Pool

type columnsSorter struct {
	columns []column
}

func (cs *columnsSorter) reset() {
	cs.columns = nil
}

func (cs *columnsSorter) Len() int {
	return len(cs.columns)
}

func (cs *columnsSorter) Less(i, j int) bool {
	columns := cs.columns
	return columns[i].name < columns[j].name
}

func (cs *columnsSorter) Swap(i, j int) {
	columns := cs.columns
	columns[i], columns[j] = columns[j], columns[i]
}

func getColumnsSorter() *columnsSorter {
	v := columnsSorterPool.Get()
	if v == nil {
		return &columnsSorter{}
	}
	return v.(*columnsSorter)
}

func putColumnsSorter(cs *columnsSorter) {
	cs.reset()
	columnsSorterPool.Put(cs)
}

var columnsSorterPool sync.Pool

type constColumnsSorter struct { // 用于做 column 的排序
	columns []Field
}

func (ccs *constColumnsSorter) reset() {
	ccs.columns = nil
}

func (ccs *constColumnsSorter) Len() int {
	return len(ccs.columns)
}

func (ccs *constColumnsSorter) Less(i, j int) bool {
	columns := ccs.columns
	return columns[i].Name < columns[j].Name // 按照 tag name 排序
}

func (ccs *constColumnsSorter) Swap(i, j int) {
	columns := ccs.columns
	columns[i], columns[j] = columns[j], columns[i]
}

func getConstColumnsSorter() *constColumnsSorter {
	v := constColumnsSorterPool.Get()
	if v == nil {
		return &constColumnsSorter{}
	}
	return v.(*constColumnsSorter)
}

func putConstColumnsSorter(ccs *constColumnsSorter) {
	ccs.reset()
	constColumnsSorterPool.Put(ccs)
}

var constColumnsSorterPool sync.Pool

// mustWriteTimestampsTo writes timestamps to sw and updates th accordingly  // 序列化时间戳
func mustWriteTimestampsTo(th *timestampsHeader, timestamps []int64, sw *streamWriters) {
	th.reset()

	bb := longTermBufPool.Get()
	// 以序列化 int 数组的方式来序列化时间
	bb.B, th.marshalType, th.minTimestamp = encoding.MarshalTimestamps(bb.B[:0], timestamps, 64)
	if len(bb.B) > maxTimestampsBlockSize {
		logger.Panicf("BUG: too big block with timestamps: %d bytes; the maximum supported size is %d bytes", len(bb.B), maxTimestampsBlockSize)
	}
	th.maxTimestamp = timestamps[len(timestamps)-1]
	th.blockOffset = sw.timestampsWriter.bytesWritten
	th.blockSize = uint64(len(bb.B))
	sw.timestampsWriter.MustWrite(bb.B) // 把时间戳写入文件
	longTermBufPool.Put(bb)
}
