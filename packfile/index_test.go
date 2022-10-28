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

	idx := NewIndex()

	idx.Add([]byte("hello"), 10)
	idx.Add([]byte("bye"), 20)
	idx.Add([]byte("world"), 30)

	n, err := idx.WriteTo(f)
	require.NoError(err)
	require.Equal(int64(2175), n)

	_, err = f.Seek(0, io.SeekStart)
	require.NoError(err)
	idx = NewIndex()

	n, err = idx.ReadFrom(f)
	require.NoError(err)
	require.Equal(int64(2175), n)

	e, err := idx.Get([]byte("hello"))
	require.NoError(err)
	require.Equal(int64(10), e.Offset)
}
