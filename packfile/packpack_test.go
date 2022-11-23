package packfile

import (
	"math/rand"
	"os"
	"path"
	"testing"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
	"github.com/stretchr/testify/require"
)

func TestWriteAndReadPackPack(t *testing.T) {
	require := require.New(t)

	dir, err := os.MkdirTemp("", "packpack")
	require.NoError(err)

	pp, err := NewPackPack(path.Join(dir, "packs"), path.Join(dir, "temp"))
	require.NoError(err)

	packProc, err := pp.NewPackProcessing(1)
	require.NoError(err)

	v1 := []byte("value1")
	err = packProc.WriteBlock([]byte("key1"), v1)
	require.NoError(err)

	v2 := []byte("value2")
	err = packProc.WriteBlock([]byte("key2"), v2)
	require.NoError(err)

	v3 := []byte("value3")
	err = packProc.WriteBlock([]byte("key3"), v3)
	require.NoError(err)

	err = packProc.Commit()
	require.NoError(err)

	val2Out, err := pp.Get(ihash.SumBytes([]byte("key2")))
	require.NoError(err)
	require.Equal(v2, val2Out)

	valNotFound, err := pp.Get(ihash.SumBytes([]byte("key22")))
	require.Nil(valNotFound)
	require.ErrorContains(err, "file does not exist")
}

var block = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}

// TODO improve benchmark reusing previous generated packfiles
func BenchmarkLookup(b *testing.B) {
	b.Run("N=100", func(b *testing.B) {
		lookup(b, 100)
	})

	b.Run("N=1000", func(b *testing.B) {
		lookup(b, 1000)
	})

	b.Run("N=10000", func(b *testing.B) {
		lookup(b, 10000)
	})

	b.Run("N=100000", func(b *testing.B) {
		lookup(b, 100000)
	})

	// b.Run("N=1000000", func(b *testing.B) {
	// 	lookup(b, 1000000)
	// })

	// b.Run("N=10000000", func(b *testing.B) {
	// 	lookup(b, 10000000)
	// })

	// b.Run("N=100000000", func(b *testing.B) {
	// 	lookup(b, 100000000)
	// })

	// b.Run("N=1000000000", func(b *testing.B) {
	// 	lookup(b, 1000000000)
	// })
}

func lookup(b *testing.B, numElements int) {
	b.Helper()

	require := require.New(b)

	dir, err := os.MkdirTemp("", "packpack")
	require.NoError(err)

	b.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Error("error cleaning up", err)
		}
	})

	pp, err := NewPackPack(path.Join(dir, "packs"), path.Join(dir, "temp"))
	require.NoError(err)

	packProc, err := pp.NewPackProcessing(1e6)
	require.NoError(err)

	var samples [][32]byte
	for i := 0; i < numElements; i++ {
		var token [32]byte
		_, err := rand.Read(token[:])
		require.NoError(err)

		if i%(numElements/100) == 0 {
			samples = append(samples, token)
		}

		err = packProc.WriteBlock(token[:], block)
		require.NoError(err)
	}

	err = packProc.Commit()
	require.NoError(err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, token := range samples {
			_, err := pp.Get(ihash.SumBytes(token[:]))
			require.NoError(err)
		}
	}
}

func BenchmarkWrite(b *testing.B) {
	b.Run("N=100", func(b *testing.B) {
		write(b, 100)
	})

	b.Run("N=1000", func(b *testing.B) {
		write(b, 1000)
	})

	b.Run("N=10000", func(b *testing.B) {
		write(b, 10000)
	})

	b.Run("N=100000", func(b *testing.B) {
		write(b, 100000)
	})

	// b.Run("N=1000000", func(b *testing.B) {
	// 	write(b, 1000000)
	// })

	// b.Run("N=10000000", func(b *testing.B) {
	// 	write(b, 10000000)
	// })

	// b.Run("N=100000000", func(b *testing.B) {
	// 	write(b, 100000000)
	// })

	// b.Run("N=1000000000", func(b *testing.B) {
	// 	write(b, 1000000000)
	// })
}

func write(b *testing.B, numElements int) {
	b.Helper()

	require := require.New(b)

	dir, err := os.MkdirTemp("", "packpack")
	require.NoError(err)

	b.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Error("error cleaning up", err)
		}
	})

	pp, err := NewPackPack(path.Join(dir, "packs"), path.Join(dir, "temp"))
	require.NoError(err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		packProc, err := pp.NewPackProcessing(1e7)
		require.NoError(err)

		for i := 0; i < numElements; i++ {
			var token [32]byte
			_, err := rand.Read(token[:])
			require.NoError(err)

			err = packProc.WriteBlock(token[:], block)
			require.NoError(err)
		}

		err = packProc.Commit()
		require.NoError(err)
	}

}
