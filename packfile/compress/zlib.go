package compress

import (
	"compress/zlib"
	"io"
)

var _ Writer = &ZlibWriter{}
var _ Reader = &ZlibReader{}

type ZlibWriter struct {
	*zlib.Writer
}

func (zw *ZlibWriter) Init(w io.Writer) {
	zw.Writer = zlib.NewWriter(w)
}

func (zw *ZlibWriter) Write(p []byte) (n int, err error) {
	n, err = zw.Writer.Write(p)
	if err != nil {
		return
	}
	return n, zw.Writer.Flush()
}

type ZlibReader struct {
	io.ReadCloser
}

func (zr *ZlibReader) Init(r io.Reader) error {
	zzr, err := zlib.NewReader(r)
	if err != nil {
		return err
	}
	zr.ReadCloser = zzr

	return nil
}

func (zr *ZlibReader) Reset(r io.Reader, dict []byte) error {
	res := zr.ReadCloser.(zlib.Resetter)
	return res.Reset(r, dict)
}
