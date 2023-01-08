package packfile

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	ihash "github.com/ajnavarro/super-blockstore/hash"
	"github.com/klauspost/compress/s2"
)

/*
format packfile:

header:
 "SPB" magic key:3 bytes
 version:uint32
blocks:
 block_header:
   key:[32]bytes (sha256)
   checksum:uint32
   blocksize:uint64
   block:[]bytes
*/

var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(nil)
	},
}

var packSig []byte = []byte{'S', 'P', 'B'}
var packVersion uint32 = 0

type Writer struct {
	w   io.Writer
	c   io.Closer
	pos int64
}

func NewWriter(w io.WriteCloser) *Writer {
	// TODO maybe buffer?
	s2w := s2.NewWriter(w, s2.WriterAddIndex())
	return &Writer{
		w: s2w,
		c: s2w,
	}
}

func (pw *Writer) WriteHeader() error {
	// header:
	//   "SPB" magic key:3 bytes
	//   version:uint32
	n, err := pw.w.Write(packSig)
	if err != nil {
		return err
	}

	pw.pos += int64(n)

	if err := binary.Write(pw.w, binary.BigEndian, packVersion); err != nil {
		return err
	}

	pw.pos += 4

	// return pw.w.Flush()
	return nil
}

func (pw *Writer) WriteBlock(key []byte, len uint32, value io.Reader) (int64, error) {
	pOut := pw.pos
	//block_header:

	//	key:[32]bytes
	k := ihash.SumBytes(key)
	n, err := pw.w.Write(k[:])
	if err != nil {
		return pOut, err
	}

	pw.pos += int64(n)

	// TODO

	//	blocksize:uint32
	if err := binary.Write(pw.w, binary.BigEndian, len); err != nil {
		return pOut, err
	}

	pw.pos += 4

	// block:

	nCopy, err := io.Copy(pw.w, value)
	if err != nil {
		return pOut, err
	}

	pw.pos += nCopy

	return pOut, nil
}

func (pw *Writer) Close() error {
	return pw.c.Close()
}
