package iio

import (
	"bytes"
	"sync"
)

var BufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}
