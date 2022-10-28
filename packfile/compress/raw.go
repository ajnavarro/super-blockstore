package compress

import (
	"io"
)

var _ Writer = &RawWriter{}
var _ Reader = &RawReader{}

type RawWriter struct {
	io.Writer
}

func (zw *RawWriter) Init(w io.Writer) {
	zw.Writer = w
}

func (zw *RawWriter) Reset(w io.Writer) {
}

type RawReader struct {
	io.Reader
}

func (zr *RawReader) Init(r io.Reader) error {
	zr.Reader = r
	return nil
}

func (zr *RawReader) Reset(r io.Reader, dict []byte) error {
	return nil
}
