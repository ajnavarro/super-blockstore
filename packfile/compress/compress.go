package compress

import "io"

type Writer interface {
	Init(w io.Writer)
	Write(p []byte) (n int, err error)
	Reset(w io.Writer)
}

type Reader interface {
	Init(w io.Reader) error
	Read(p []byte) (n int, err error)
	Reset(r io.Reader, dict []byte) error
}
