package packfile

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteAndReadIndex(t *testing.T) {
	require := require.New(t)

	f, err := os.CreateTemp("", "test.idx")
	require.NoError(err)

	idx := NewIndexWriter()

	idx.Add([]byte("hello"), 10, 100)
	idx.Add([]byte("bye"), 20, 200)
	idx.Add([]byte("world"), 30, 300)

	n, err := idx.WriteTo(f)
	require.NoError(err)
	require.Equal(int64(2199), n)

	_, err = f.Seek(0, io.SeekStart)
	require.NoError(err)
	idReader := NewIndexReader()

	n, err = idReader.ReadFrom(f)
	require.NoError(err)
	require.Equal(int64(2199), n)

	// e, err := idReader.Get([]byte("hello"))
	// require.NoError(err)
	// require.Equal(int64(10), e.Offset)
	// require.Equal(int64(100), e.Size)

}
