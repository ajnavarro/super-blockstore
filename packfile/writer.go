package packfile

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"io"
	"sync"

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
	w   *bufio.Writer
	c   io.Closer
	pos int64

	h hash.Hash
}

func NewWriter(w io.WriteCloser) *Writer {
	h := sha256.New()
	return &Writer{
		w: bufio.NewWriter(io.MultiWriter(w, h)),
		c: w,
		h: h,
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

	return pw.w.Flush()
}

func (pw *Writer) WriteBlock(key []byte, len int64, value io.Reader) (int64, error) {
	pOut := pw.pos
	//block_header:

	//	key:[32]bytes
	k := sha256.Sum256(key)
	n, err := pw.w.Write(k[:])
	if err != nil {
		return pOut, err
	}

	pw.pos += int64(n)

	// TODO

	//	blocksize:uint64
	if err := binary.Write(pw.w, binary.BigEndian, uint64(len)); err != nil {
		return pOut, err
	}

	pw.pos += 8

	// block:

	nCopy, err := io.Copy(pw.w, value)
	if err != nil {
		return pOut, err
	}

	pw.pos += nCopy

	return pOut, pw.w.Flush()
}

func (pw *Writer) Hash() string {
	return hex.EncodeToString(pw.h.Sum(nil))
}

func (pw *Writer) Close() error {
	return pw.c.Close()
}

// TODO integrate with standard packfile Writer
type WriterSnappy struct {
	*Writer
	zr *s2.Writer
}

func NewWriterSnappy(pr *Writer) *WriterSnappy {
	return &WriterSnappy{
		Writer: pr,
		zr:     s2.NewWriter(nil),
	}
}

func (pw *WriterSnappy) WriteBlock(key []byte, value io.Reader) (int64, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	pw.zr.Reset(buf)

	_, err := io.Copy(pw.zr, value)
	if err != nil {
		return 0, err
	}

	if err := pw.zr.Flush(); err != nil {
		return 0, err
	}

	n, err := pw.Writer.WriteBlock(key, int64(buf.Len()), buf)

	bufPool.Put(buf)

	return n, err
}

func (pw *WriterSnappy) Close() error {
	if err := pw.zr.Close(); err != nil {
		return err
	}

	return pw.Writer.Close()
}
