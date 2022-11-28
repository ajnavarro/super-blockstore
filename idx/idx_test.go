package idx

import (
	"io"
	"os"
	"testing"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
	"github.com/stretchr/testify/require"
)

func TestWriteAndReadIndex(t *testing.T) {
	require := require.New(t)

	f, err := os.CreateTemp("", "test.idx")
	require.NoError(err)

	idx := NewIndexWriter()

	idx.Add([]byte("hello"), 1, 10, 100)
	idx.Add([]byte("bye"), 2, 20, 200)
	idx.Add([]byte("world"), 3, 30, 300)

	n, err := idx.WriteTo(f)
	require.NoError(err)
	require.Equal(int64(1163), n)

	_, err = f.Seek(0, io.SeekStart)
	require.NoError(err)
	idReader := NewIndexReader()

	n, err = idReader.ReadFrom(f)
	require.NoError(err)
	require.Equal(int64(1163), n)

	key := ihash.SumBytes([]byte("hello"))

	contains, err := idReader.Contains(key)
	require.NoError(err)
	require.True(contains)

	crc, err := idReader.GetCRC32(key)
	require.NoError(err)
	offset, err := idReader.GetOffset(key)
	require.NoError(err)

	size, err := idReader.GetSize(key)
	require.NoError(err)

	require.Equal(int64(10), offset)
	require.Equal(uint32(100), size)
	require.Equal(uint32(1), crc)

}
