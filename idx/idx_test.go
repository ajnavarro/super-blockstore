package idx

import (
	"io"
	"os"
	"testing"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
	"github.com/stretchr/testify/require"
)

func TestWriteAndReadIndex_OLD(t *testing.T) {
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

var indexFixtures = []struct {
	Name        string
	GetInstance func(path string) (Idx, error)
}{
	{
		Name: "multi",
		GetInstance: func(path string) (Idx, error) {
			return NewMulti(path, path, 10)
		},
	},
}

func TestReadWriteIdx(t *testing.T) {
	for _, i := range indexFixtures {
		t.Run(i.Name, func(t *testing.T) {
			require := require.New(t)

			idx, err := i.GetInstance(t.TempDir())
			require.NoError(err)

			packName := "packnameTEST"

			tx, err := idx.NewTransaction(packName)
			require.NoError(err)

			k1 := ihash.SumBytes([]byte("hello"))
			k2 := ihash.SumBytes([]byte("bye"))
			k3 := ihash.SumBytes([]byte("world"))
			tx.Add(k1, 1, 10, 100)
			tx.Add(k2, 2, 20, 200)
			tx.Add(k3, 3, 30, 300)

			require.NoError(tx.Commit())

			ok, err := idx.Contains(k1)
			require.NoError(err)
			require.True(ok)

			// TODO GET CRC32

			pn, offs, err := idx.GetOffset(k1)
			require.NoError(err)
			require.Equal(packName, pn)
			require.Equal(int64(10), offs)

			size, err := idx.GetSize(k1)
			require.NoError(err)
			require.Equal(uint32(100), size)

			err = idx.DeleteAll(packName)
			require.NoError(err)

			_, _, err = idx.GetOffset(k1)
			require.Error(err)
		})
	}

}
