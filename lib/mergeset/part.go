package mergeset

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/blockcache"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
)

var idxbCache = blockcache.NewCache(getMaxIndexBlocksCacheSize)
var ibCache = blockcache.NewCache(getMaxInmemoryBlocksCacheSize) // ??? 还没搞懂这个 cache 如何运作的

// SetIndexBlocksCacheSize overrides the default size of indexdb/indexBlock cache
func SetIndexBlocksCacheSize(size int) {
	maxIndexBlockCacheSize = size
}

func getMaxIndexBlocksCacheSize() int {
	maxIndexBlockCacheSizeOnce.Do(func() {
		if maxIndexBlockCacheSize <= 0 {
			maxIndexBlockCacheSize = int(0.10 * float64(memory.Allowed()))
		}
	})
	return maxIndexBlockCacheSize
}

var (
	maxIndexBlockCacheSize     int
	maxIndexBlockCacheSizeOnce sync.Once
)

// SetDataBlocksCacheSize overrides the default size of indexdb/dataBlocks cache
func SetDataBlocksCacheSize(size int) {
	maxInmemoryBlockCacheSize = size
}

func getMaxInmemoryBlocksCacheSize() int {
	maxInmemoryBlockCacheSizeOnce.Do(func() {
		if maxInmemoryBlockCacheSize <= 0 {
			maxInmemoryBlockCacheSize = int(0.25 * float64(memory.Allowed()))
		}
	})
	return maxInmemoryBlockCacheSize
}

var (
	maxInmemoryBlockCacheSize     int
	maxInmemoryBlockCacheSizeOnce sync.Once
)

type part struct {
	ph partHeader

	path string

	size uint64

	mrs []metaindexRow

	indexFile fs.MustReadAtCloser
	itemsFile fs.MustReadAtCloser
	lensFile  fs.MustReadAtCloser
}

// PartReader 用于读一个 part
type PartReader struct {
	Path          string
	TableType     int8            // prev 还是 curr
	PartHeader    partHeader      // metadata.json
	MetaIndexRows []metaindexRow  // metaindex.bin
	BlockHeaders  [][]blockHeader // index.bin
	ItemsFile     *fs.ReaderAt    // items.bin
	LensFile      *fs.ReaderAt    // lens.bin
	Cache         sync.Map
	//
	sb storageBlock
}

type CacheItem struct {
	Ref     uint64
	IB      *inmemoryBlock
	LastUse int64
}

func (c *CacheItem) AddRef() {
	atomic.AddUint64(&c.Ref, 1)
	c.LastUse = time.Now().Unix()
}

func (c *CacheItem) DeRef() {
	if atomic.AddUint64(&c.Ref, ^uint64(0)) == 0 {
		c.IB.Reset()
		c.IB = nil
		c.LastUse = 0
		// todo: 从 map 中删除
	}
}

func NewPartReader(path string, tableType int8) (*PartReader, error) {
	p := &PartReader{
		Path:      path,
		TableType: tableType,
	}
	p.PartHeader.MustReadMetadata(path)
	metaindexBin, err := os.ReadFile(filepath.Join(path, metaindexFilename))
	if err != nil {
		return nil, fmt.Errorf("open metaindex.bin error, err=%w", err)
	}
	mrs, err := unmarshalMetaindexRows(nil, bytes.NewReader(metaindexBin)) // mrs 一定是有序的
	if err != nil {
		logger.Panicf("FATAL: cannot unmarshal metaindexRows from %q: %s", path, err)
	}
	p.MetaIndexRows = mrs
	//
	var indexBin []byte
	indexBin, err = os.ReadFile(filepath.Join(path, indexFilename))
	if err != nil {
		return nil, fmt.Errorf("open index.bin error, err=%w", err)
	}
	var zstdBuf []byte
	p.BlockHeaders = make([][]blockHeader, 0, len(mrs))
	for _, mr := range mrs {
		row := indexBin[mr.indexBlockOffset : mr.indexBlockOffset+uint64(mr.indexBlockSize)]
		zstdBuf, err = encoding.DecompressZSTD(zstdBuf[:0], row)
		if err != nil {
			return nil, fmt.Errorf("DecompressZSTD index.bin error, err=%w", err)
		}
		var bhs []blockHeader
		bhs, err = unmarshalBlockHeadersNoCopy(nil, zstdBuf, int(mr.blockHeadersCount)) // bhs 一定是有序的
		if err != nil {
			return nil, fmt.Errorf("unmarshalBlockHeadersNoCopy for index.bin error, err=%w", err)
		}
		p.BlockHeaders = append(p.BlockHeaders, bhs)
	}
	//
	p.ItemsFile = fs.MustOpenReaderAt(filepath.Join(path, itemsFilename)) //todo: 为了减少全局的 io, 要为 block 对象建立 cache
	p.LensFile = fs.MustOpenReaderAt(filepath.Join(path, lensFilename))
	return p, nil
}

func (p *PartReader) Close() {
	p.ItemsFile.MustClose()
	p.LensFile.MustClose()
}

func (p *PartReader) GetInmemoryBlock(rowIndex, blockIndex int) *CacheItem {
	if rowIndex < 0 || rowIndex >= len(p.MetaIndexRows) {
		panic("row index out of range")
	}
	//mr := p.MetaIndexRows[rowIndex]
	bhs := p.BlockHeaders[rowIndex]
	if blockIndex < 0 || blockIndex >= len(bhs) {
		panic("block index out of range")
	}
	bh := &bhs[blockIndex]
	//
	cacheIndex := uint64(((rowIndex & 0xFFFFFFFF) << 32) | (blockIndex & 0xFFFFFFFF))
	ret, has := p.Cache.Load(cacheIndex)
	if has {
		return ret.(*CacheItem)
	}
	// todo: 考虑剩余内存
	ib, err := p.readInmemoryBlock(bh)
	if err != nil {
		panic("readInmemoryBlock error, err=" + err.Error())
	}
	c := &CacheItem{
		Ref:     1,
		IB:      ib,
		LastUse: time.Now().Unix(),
	}
	p.Cache.Store(cacheIndex, c)
	return c
}

func (p *PartReader) readInmemoryBlock(bh *blockHeader) (*inmemoryBlock, error) { // 从磁盘读取一个块  // todo: ib 应该要放在 cache 里面
	p.sb.Reset()
	var sb storageBlock
	sb.Reset()
	p.sb.itemsData = bytesutil.ResizeNoCopyMayOverallocate(p.sb.itemsData, int(bh.itemsBlockSize))
	sb.itemsData = p.ItemsFile.ReadAtNocopy(p.sb.itemsData, int64(bh.itemsBlockOffset)) // 直接使用 mmap 的内存，减少拷贝
	p.sb.lensData = bytesutil.ResizeNoCopyMayOverallocate(p.sb.lensData, int(bh.lensBlockSize))
	sb.lensData = p.LensFile.ReadAtNocopy(p.sb.lensData, int64(bh.lensBlockOffset))
	ib := getInmemoryBlock()
	if err := ib.UnmarshalData(&sb, bh.firstItem, bh.commonPrefix, bh.itemsCount, bh.marshalType); err != nil {
		sb.itemsData = nil
		sb.lensData = nil
		return nil, fmt.Errorf("cannot unmarshal storage block with %d items: %w", bh.itemsCount, err)
	}
	sb.itemsData = nil
	sb.lensData = nil
	return ib, nil
}

func mustOpenFilePart(path string) *part {
	var ph partHeader
	ph.MustReadMetadata(path)

	metaindexPath := filepath.Join(path, metaindexFilename)
	metaindexFile := filestream.MustOpen(metaindexPath, true)
	metaindexSize := fs.MustFileSize(metaindexPath)

	indexPath := filepath.Join(path, indexFilename)
	indexFile := fs.MustOpenReaderAt(indexPath)
	indexSize := fs.MustFileSize(indexPath)

	itemsPath := filepath.Join(path, itemsFilename)
	itemsFile := fs.MustOpenReaderAt(itemsPath)
	itemsSize := fs.MustFileSize(itemsPath)

	lensPath := filepath.Join(path, lensFilename)
	lensFile := fs.MustOpenReaderAt(lensPath)
	lensSize := fs.MustFileSize(lensPath)

	size := metaindexSize + indexSize + itemsSize + lensSize
	return newPart(&ph, path, size, metaindexFile, indexFile, itemsFile, lensFile)
}

func newPart(ph *partHeader, path string, size uint64, metaindexReader filestream.ReadCloser, indexFile, itemsFile, lensFile fs.MustReadAtCloser) *part {
	mrs, err := unmarshalMetaindexRows(nil, metaindexReader)
	if err != nil {
		logger.Panicf("FATAL: cannot unmarshal metaindexRows from %q: %s", path, err)
	}
	metaindexReader.MustClose()

	var p part
	p.path = path
	p.size = size
	p.mrs = mrs

	p.indexFile = indexFile
	p.itemsFile = itemsFile
	p.lensFile = lensFile

	p.ph.CopyFrom(ph)
	return &p
}

func (p *part) MustClose() {
	p.indexFile.MustClose()
	p.itemsFile.MustClose()
	p.lensFile.MustClose()

	idxbCache.RemoveBlocksForPart(p)
	ibCache.RemoveBlocksForPart(p)
}

type indexBlock struct {
	bhs []blockHeader

	// The buffer for holding the data referrred by bhs
	buf []byte
}

func (idxb *indexBlock) SizeBytes() int {
	bhs := idxb.bhs[:cap(idxb.bhs)]
	n := int(unsafe.Sizeof(*idxb))
	for i := range bhs {
		n += bhs[i].SizeBytes()
	}
	return n
}

type PartCursor struct {
	Part       *PartReader
	RowIndex   int
	BlockIndex int
	ItemIndex  int
	ib         *CacheItem
	err        error
	Current    []byte
}

func NewPartCursor(p *PartReader) *PartCursor {
	inst := &PartCursor{
		Part:       p,
		RowIndex:   0,
		BlockIndex: 0,
		ItemIndex:  0,
	}
	inst.ib = p.GetInmemoryBlock(0, 0)
	return inst
}

func (pc *PartCursor) Next() bool {
	if pc.err != nil {
		return false
	}
	ib := pc.ib.IB
	if pc.ItemIndex >= 0 && pc.ItemIndex < len(ib.items) {
		pc.Current = ib.items[pc.ItemIndex].Bytes(ib.data)
		pc.ItemIndex++
		return true
	}
	return pc.nextBlock()
}

func (pc *PartCursor) nextBlock() bool {
	pc.Current = nil
	pc.ItemIndex = -1
	pc.ib = nil
	bhs := pc.Part.BlockHeaders[pc.RowIndex]
	if pc.BlockIndex < len(bhs)-1 {
		pc.BlockIndex++
		pc.ib = pc.Part.GetInmemoryBlock(pc.RowIndex, pc.BlockIndex)
		pc.ItemIndex = 0
		pc.Current = nil
		return true
	}
	return pc.nextMetaIndexRow()
}

func (pc *PartCursor) nextMetaIndexRow() bool {
	if pc.RowIndex < len(pc.Part.MetaIndexRows)-1 {
		pc.RowIndex++
		return pc.nextBlock()
	}
	pc.err = io.EOF
	return false
}
