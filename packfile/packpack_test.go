package packfile

import (
	"bytes"
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
	err = packProc.WriteBlock([]byte("key1"), int64(len(v1)), bytes.NewBuffer(v1))
	require.NoError(err)

	v2 := []byte("value2")
	err = packProc.WriteBlock([]byte("key2"), int64(len(v2)), bytes.NewBuffer(v2))
	require.NoError(err)

	v3 := []byte("value3")
	err = packProc.WriteBlock([]byte("key3"), int64(len(v3)), bytes.NewBuffer(v3))
	require.NoError(err)

	err = packProc.Commit()
	require.NoError(err)

	val2Out, err := pp.GetHash(ihash.SumBytes([]byte("key2")))
	require.NoError(err)
	require.Equal(v2, val2Out)

	valNotFound, err := pp.GetHash(ihash.SumBytes([]byte("key22")))
	require.Nil(valNotFound)
	require.ErrorContains(err, "file does not exist")
}
