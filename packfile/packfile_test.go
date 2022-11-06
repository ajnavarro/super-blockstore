package packfile

import (
	"bytes"
	"io"
	"io/ioutil"
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

	pos1, err := pw.WriteBlock([]byte("hello"), bytes.NewReader([]byte("world")))
	require.NoError(err)
	require.Equal(int64(7), pos1)
	pos2, err := pw.WriteBlock([]byte("ttt"), bytes.NewReader([]byte("somevalue")))
	require.NoError(err)
	require.Equal(int64(70), pos2)
	pos3, err := pw.WriteBlock([]byte("bye"), bytes.NewBuffer([]byte("cruel world")))
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

// func TestCopyPackfileSnappy(t *testing.T) {
// 	f, err := os.OpenFile("wikipedias2.pack", os.O_RDONLY, 0755)
// 	if err != nil {
// 		panic(err)
// 	}

// 	fcopy, err := os.OpenFile("wikipedia-copys2best.pack", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
// 	if err != nil {
// 		panic(err)
// 	}

// 	pr := NewReaderSnappy(NewReader(f))

// 	pw := NewWriterSnappy(NewWriter(fcopy))

// 	err = pw.WriteHeader()
// 	if err != nil {
// 		panic(err)
// 	}

// 	for {
// 		k, v, err := pr.Next()
// 		if errors.Is(err, io.EOF) {
// 			break
// 		}
// 		if err != nil {
// 			panic(err)
// 		}
// 		// TODO allow raw copy
// 		_, err = pw.WriteBlock(k, bytes.NewReader(v))
// 		if err != nil {
// 			panic(err)
// 		}
// 	}

// 	pw.Close()
// 	f.Close()
// }
