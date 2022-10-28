package compress

import (
	"io"

	"github.com/klauspost/compress/s2"
)

var _ Writer = &SnappyWriter{}
var _ Reader = &SnappyReader{}

type SnappyWriter struct {
	*s2.Writer
}

func (zw *SnappyWriter) Init(w io.Writer) {
	zw.Writer = s2.NewWriter(w)
}

type SnappyReader struct {
	*s2.Reader
}

func (zr *SnappyReader) Init(r io.Reader) error {
	zr.Reader = s2.NewReader(r)
	return nil
}

func (zr *SnappyReader) Reset(r io.Reader, dict []byte) error {
	zr.Reader.Reset(r)
	return nil
}
