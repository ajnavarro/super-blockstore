package idx

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
	"github.com/ajnavarro/super-blockstorage/iio"
)

var _ io.WriterTo = &IndexWriter{}

type IndexWriter struct {
	added   map[ihash.Hash]struct{}
	entries Entries

	fanoutTable   []uint32
	fanoutMapping [256]int

	names     [][]byte
	crcs32    [][]byte
	offsets32 [][]byte
	offsets64 []byte
	sizes     [][]byte

	version       uint32
	offset64Write uint32
}

func NewIndexWriter() *IndexWriter {
	return &IndexWriter{
		fanoutTable: make([]uint32, fanoutSize),
	}
}

func (idx *IndexWriter) Add(key []byte, crc32 uint32, pos uint64, size uint32) {
	idx.AddRaw(ihash.SumBytes(key), crc32, pos, size)
}

func (idx *IndexWriter) AddRaw(h ihash.Hash, crc32 uint32, pos uint64, size uint32) {
	if idx.added == nil {
		idx.added = make(map[ihash.Hash]struct{})
	}

	if _, ok := idx.added[h]; !ok {
		idx.added[h] = struct{}{}
		idx.entries = append(idx.entries,
			&Entry{
				Key:    h,
				CRC32:  crc32,
				Offset: pos,
				Size:   size,
			},
		)
	}
}

func (idx *IndexWriter) Count() int64 {
	return int64(len(idx.entries))
}

func (idx *IndexWriter) WriteTo(writer io.Writer) (int64, error) {
	if err := idx.prepareData(); err != nil {
		return 0, err
	}

	flow := []func(io.Writer) (int, error){
		idx.writeSignature,
		idx.writeVersion,
		idx.writeFanout,
		idx.writeNames,
		idx.writeCRC,
		idx.writeOffsets,
		idx.writeSizes,
		// TODO footer with checksum
	}

	var nOut int64
	for _, e := range flow {
		n, err := e(writer)
		nOut += int64(n)
		if err != nil {
			return nOut, err
		}
	}

	return nOut, nil
}

func (idx *IndexWriter) prepareData() error {
	SortEntriesByHash(idx.entries)

	for i := range idx.fanoutMapping {
		idx.fanoutMapping[i] = noMapping
	}

	buf := new(bytes.Buffer)

	last := -1
	bucket := -1
	for i, o := range idx.entries {
		fan := o.Key[0]

		// fill the gaps between fans
		for j := last + 1; j < int(fan); j++ {
			idx.fanoutTable[j] = uint32(i)
		}

		// update the number of objects for this position
		idx.fanoutTable[fan] = uint32(i + 1)

		// we move from one bucket to another, update counters and allocate
		// memory
		if last != int(fan) {
			bucket++
			idx.fanoutMapping[fan] = bucket
			last = int(fan)

			idx.names = append(idx.names, make([]byte, 0))
			idx.offsets32 = append(idx.offsets32, make([]byte, 0))
			idx.crcs32 = append(idx.crcs32, make([]byte, 0))
			idx.sizes = append(idx.sizes, make([]byte, 0))
		}

		idx.names[bucket] = append(idx.names[bucket], o.Key[:]...)

		offset := o.Offset
		if offset > math.MaxInt32 {
			offset = idx.addOffset64(offset)
		}

		buf.Truncate(0)
		if err := binary.Write(buf, binary.BigEndian, uint32(offset)); err != nil {
			return err
		}
		idx.offsets32[bucket] = append(idx.offsets32[bucket], buf.Bytes()...)

		buf.Truncate(0)
		if err := binary.Write(buf, binary.BigEndian, &o.CRC32); err != nil {
			return err
		}
		idx.crcs32[bucket] = append(idx.crcs32[bucket], buf.Bytes()...)

		buf.Truncate(0)
		if err := binary.Write(buf, binary.BigEndian, &o.Size); err != nil {
			return err
		}
		idx.sizes[bucket] = append(idx.sizes[bucket], buf.Bytes()...)
	}

	for j := last + 1; j < 256; j++ {
		idx.fanoutTable[j] = uint32(len(idx.entries))
	}

	idx.version = indexVersion
	// TODO write checksum

	return nil
}

func (idx *IndexWriter) addOffset64(pos uint64) uint64 {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, &pos); err != nil {
		return 0
	}

	idx.offsets64 = append(idx.offsets64, buf.Bytes()...)

	index := uint64(idx.offset64Write | (1 << 31))
	idx.offset64Write++

	return index
}

func (idx *IndexWriter) writeSignature(w io.Writer) (int, error) {
	return w.Write(indexSig)
}

func (idx *IndexWriter) writeVersion(w io.Writer) (int, error) {
	if err := binary.Write(w, binary.BigEndian, &indexVersion); err != nil {
		return 0, err
	}

	return 4, nil
}

func (idx *IndexWriter) writeFanout(w io.Writer) (int, error) {
	for _, c := range idx.fanoutTable {
		if err := binary.Write(w, binary.BigEndian, &c); err != nil {
			return 0, err
		}
	}

	return fanoutSize * 4, nil
}

func (idx *IndexWriter) writeNames(w io.Writer) (int, error) {
	var size int
	for k := 0; k < fanoutSize; k++ {
		pos := idx.fanoutMapping[k]
		if pos == noMapping {
			continue
		}

		n, err := w.Write(idx.names[pos])
		if err != nil {
			return size, err
		}
		size += n
	}

	return size, nil
}

func (idx *IndexWriter) writeCRC(w io.Writer) (int, error) {
	var size int
	for k := 0; k < fanoutSize; k++ {
		pos := idx.fanoutMapping[k]
		if pos == noMapping {
			continue
		}

		n, err := w.Write(idx.crcs32[pos])
		if err != nil {
			return size, err
		}

		size += n
	}

	return size, nil
}

func (idx *IndexWriter) writeOffsets(w io.Writer) (int, error) {
	var size int
	for k := 0; k < fanoutSize; k++ {
		pos := idx.fanoutMapping[k]
		if pos == noMapping {
			continue
		}

		n, err := w.Write(idx.offsets32[pos])
		if err != nil {
			return size, err
		}

		size += n
	}

	if len(idx.offsets64) > 0 {
		n, err := w.Write(idx.offsets64)
		if err != nil {
			return size, err
		}

		size += n
	}

	return size, nil
}

func (idx *IndexWriter) writeSizes(w io.Writer) (int, error) {
	var size int
	for k := 0; k < fanoutSize; k++ {
		pos := idx.fanoutMapping[k]
		if pos == noMapping {
			continue
		}

		n, err := w.Write(idx.sizes[pos])
		if err != nil {
			return size, err
		}

		size += n
	}

	return size, nil
}

func WriteIndex(i *IndexWriter, path string) error {
	f, err := iio.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	_, err = i.WriteTo(f)
	if err != nil {
		return err
	}

	return f.Close()
}
