package packfile

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTombstone(t *testing.T) {
	require := require.New(t)
	f, err := os.CreateTemp("", "tombstone.bin")
	require.NoError(err)

	filename := f.Name()

	ts, err := NewTombstonePath(filename)
	require.NoError(err)
	require.NotNil(ts)

	err = ts.AddKey([]byte("a"))
	require.NoError(err)

	err = ts.AddKey([]byte("b"))
	require.NoError(err)

	err = ts.AddKey([]byte("c"))
	require.NoError(err)

	ok, err := ts.Has([]byte("b"))
	require.NoError(err)
	require.True(ok)

	ok, err = ts.Has([]byte("z"))
	require.NoError(err)
	require.False(ok)

	require.NoError(ts.Close())

	ts2, err := NewTombstonePath(filename)
	require.NoError(err)

	ok, err = ts2.Has([]byte("c"))
	require.NoError(err)
	require.True(ok)

	err = ts2.Clear()
	require.NoError(err)

	ok, err = ts2.Has([]byte("z"))
	require.NoError(err)
	require.False(ok)
}

func BenchmarkTombstoneWrite(b *testing.B) {
	require := require.New(b)
	f, err := os.CreateTemp("", "tombstone.bin")
	require.NoError(err)

	filename := f.Name()

	ts, err := NewTombstonePath(filename)
	require.NoError(err)
	require.NotNil(ts)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		require.NoError(ts.AddKey(b))
	}

	ts.Close()
}

func BenchmarkTombstoneRead(b *testing.B) {
	require := require.New(b)
	f, err := os.CreateTemp("", "tombstone.bin")
	require.NoError(err)

	filename := f.Name()

	ts, err := NewTombstonePath(filename)
	require.NoError(err)
	require.NotNil(ts)

	for i := 0; i < b.N; i++ {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		require.NoError(ts.AddKey(b))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		ok, err := ts.Has(b)
		require.NoError(err)
		require.True(ok)
	}

	ts.Close()
}
