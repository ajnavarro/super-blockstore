package packfile

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
	"github.com/stretchr/testify/require"
)

func TestWriteAndReadPackfile(t *testing.T) {
	require := require.New(t)
	f, err := os.CreateTemp("", "test.pack")
	require.NoError(err)

	pw := NewWriter(f)

	err = pw.WriteHeader()
	require.NoError(err)

	pos1, err := pw.WriteBlock([]byte("hello"), 5, bytes.NewReader([]byte("world")))
	require.NoError(err)
	require.Equal(int64(7), pos1)
	pos2, err := pw.WriteBlock([]byte("ttt"), 9, bytes.NewReader([]byte("somevalue")))
	require.NoError(err)
	require.Equal(int64(52), pos2)
	pos3, err := pw.WriteBlock([]byte("bye"), 11, bytes.NewBuffer([]byte("cruel world")))
	require.NoError(err)
	require.Equal(int64(101), pos3)

	_, err = f.Seek(0, io.SeekStart)
	require.NoError(err)

	pr := NewReader(f)
	key, vr, err := pr.Next()
	require.NoError(err)

	k1 := ihash.SumBytes([]byte("hello"))
	require.Equal(k1[:], key)
	value, err := io.ReadAll(vr)
	require.NoError(err)
	require.Equal([]byte("world"), value)

	err = pr.Skip()
	require.NoError(err)

	key, vr, err = pr.Next()
	require.NoError(err)

	k3 := ihash.SumBytes([]byte("bye"))
	require.Equal(k3[:], key)
	value, err = io.ReadAll(vr)
	require.NoError(err)
	require.Equal([]byte("cruel world"), value)

	k2, v2r, err := pr.ReadValueAt(pos2)
	hk2 := ihash.SumBytes([]byte("ttt"))
	require.NoError(err)
	require.Equal(hk2[:], k2)

	v2, err := ioutil.ReadAll(v2r)
	require.NoError(err)
	require.Equal("somevalue", string(v2))

}

func TestWriteAndReadPackfileSnappy(t *testing.T) {
	require := require.New(t)
	f, err := os.CreateTemp("", "test.pack")
	require.NoError(err)

	pw := NewWriterSnappy(NewWriter(f))

	err = pw.WriteHeader()
	require.NoError(err)

	pos1, err := pw.WriteBlock([]byte("hello"), []byte("world"))
	require.NoError(err)
	require.Equal(int64(7), pos1)
	pos2, err := pw.WriteBlock([]byte("ttt"), []byte("somevalue"))
	require.NoError(err)
	require.Equal(int64(70), pos2)
	pos3, err := pw.WriteBlock([]byte("bye"), []byte("cruel world"))
	require.NoError(err)
	require.Equal(int64(137), pos3)

	_, err = f.Seek(0, io.SeekStart)
	require.NoError(err)

	pr := NewReaderSnappy(NewReader(f))
	key, value, err := pr.Next()
	require.NoError(err)

	k1 := ihash.SumBytes([]byte("hello"))
	require.Equal(k1[:], key)
	require.Equal([]byte("world"), value)

	err = pr.Skip()
	require.NoError(err)

	key, value, err = pr.Next()
	require.NoError(err)

	k3 := ihash.SumBytes([]byte("bye"))
	require.Equal(k3[:], key)
	require.Equal([]byte("cruel world"), value)

	k2, v2, err := pr.ReadValueAt(pos2)
	hk2 := ihash.SumBytes([]byte("ttt"))
	require.NoError(err)
	require.Equal(hk2[:], k2)

	require.Equal("somevalue", string(v2))

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

		pw := NewWriterSnappy(NewWriter(f))

		err = pw.WriteHeader()
		require.NoError(err)

		b.StartTimer()

		for i := 0; i < numBlocks; i++ {
			_, err = pw.WriteBlock(tokens[i][:], block)
			require.NoError(err)
		}

		err = pw.Close()
		require.NoError(err)
	}
}
