package storage

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
)

func TestWriteAndReadObjects(t *testing.T) {
	require := require.New(t)

	dir, err := os.MkdirTemp("", "object-storage")
	require.NoError(err)

	k1 := ihash.SumBytes([]byte("testKey1"))
	v1 := []byte("testData1")

	k2 := ihash.SumBytes([]byte("testKey2"))
	v2 := []byte("testData2")

	os := NewObjectStorage(dir)

	err = os.Add(k1, v1)
	require.NoError(err)

	err = os.Add(k2, v2)
	require.NoError(err)

	val, err := os.Get(k1)
	require.NoError(err)

	require.Equal(v1, val)

	iter, err := os.GetAll()
	require.NoError(err)

	count := 0
	for {
		key, val, err := iter.Next()
		if err == io.EOF {
			break
		}
		require.NoError(err)

		if bytes.Equal(key[:], k1[:]) {
			require.Equal(v1, val)
		} else if bytes.Equal(key[:], k2[:]) {
			require.Equal(v2, val)
		} else {
			require.Fail("unexpected key", "keys %v, %v, %v", key, k1, k2)
		}

		count++
	}

	require.Equal(2, count)

	err = os.DeleteAll()
	require.NoError(err)
}
