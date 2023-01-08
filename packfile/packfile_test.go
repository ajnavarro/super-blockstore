package packfile

import (
	"bytes"
	"math/rand"
	"os"
	"testing"

	ihash "github.com/ajnavarro/super-blockstore/hash"
	"github.com/stretchr/testify/require"
)

func TestWriteAndReadPackfile(t *testing.T) {
	require := require.New(t)
	f, err := os.CreateTemp("", "test.pack")
	fname := f.Name()
	require.NoError(err)

	pw := NewWriter(f)

	err = pw.WriteHeader()
	require.NoError(err)

	pos1, err := pw.WriteBlock([]byte("hello"), 5, bytes.NewReader([]byte("world")))
	require.NoError(err)
	require.Equal(int64(7), pos1)
	pos2, err := pw.WriteBlock([]byte("ttt"), 9, bytes.NewReader([]byte("somevalue")))
	require.NoError(err)
	require.Equal(int64(48), pos2)
	pos3, err := pw.WriteBlock([]byte("bye"), 11, bytes.NewBuffer([]byte("cruel world")))
	require.NoError(err)
	require.Equal(int64(93), pos3)

	err = pw.Close()
	require.NoError(err)

	f, err = os.Open(fname)
	require.NoError(err)

	pr, err := NewReader(f)
	require.NoError(err)

	key, vr, err := pr.Next()
	require.NoError(err)

	k1 := ihash.SumBytes([]byte("hello"))
	require.Equal(k1[:], key)
	require.Equal([]byte("world"), vr)

	err = pr.Skip()
	require.NoError(err)

	key, vr, err = pr.Next()
	require.NoError(err)

	k3 := ihash.SumBytes([]byte("bye"))
	require.Equal(k3[:], key)
	require.Equal([]byte("cruel world"), vr)

	k2, v2r, err := pr.ReadValueAt(pos2)
	hk2 := ihash.SumBytes([]byte("ttt"))
	require.NoError(err)
	require.Equal(hk2[:], k2)

	require.Equal("somevalue", string(v2r))

}

func BenchmarkPackfileWrite(b *testing.B) {
	b.Run("N=1", func(b *testing.B) {
		packfileWriteElements(b, 1)
	})

	b.Run("N=10", func(b *testing.B) {
		packfileWriteElements(b, 10)
	})

	b.Run("N=1000", func(b *testing.B) {
		packfileWriteElements(b, 1000)
	})

	b.Run("N=10000", func(b *testing.B) {
		packfileWriteElements(b, 10000)
	})

	b.Run("N=100000", func(b *testing.B) {
		packfileWriteElements(b, 100000)
	})

	b.Run("N=1000000", func(b *testing.B) {
		packfileWriteElements(b, 1000000)
	})
}

const blockLen = 1204 * 1024

func generateBlock() []byte {
	var block [blockLen]byte
	_, err := rand.Read(block[:])
	if err != nil {
		panic(err)
	}

	return block[:]
}

func packfileWriteElements(b *testing.B, numBlocks int) {
	b.Helper()

	require := require.New(b)

	tokens := make([][32]byte, numBlocks)
	for i := 0; i < numBlocks; i++ {
		var token [32]byte
		_, err := rand.Read(token[:])
		require.NoError(err)
		tokens = append(tokens, token)
	}

	block := generateBlock()

	b.ResetTimer()

	b.SetBytes(int64((32 + blockLen) * numBlocks))

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		f, err := os.CreateTemp("", "test.pack")
		require.NoError(err)

		pw := NewWriter(f)

		err = pw.WriteHeader()
		require.NoError(err)

		b.StartTimer()

		for i := 0; i < numBlocks; i++ {
			_, err = pw.WriteBlock(tokens[i][:], uint32(len(block)), bytes.NewBuffer(block))
			require.NoError(err)
		}

		err = pw.Close()
		require.NoError(err)
	}
}
