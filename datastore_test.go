package superblock

import (
	"context"
	"os"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/require"
)

func TestWriteAndReadSingleBlock(t *testing.T) {
	require := require.New(t)

	dir, err := os.MkdirTemp("", "super_datastore")
	require.NoError(err)

	ds, err := NewDatastore(&DatastoreConfig{
		Folder:                dir,
		BlockCacheNumElements: 100,
	})
	require.NoError(err)

	ctx := context.Background()

	key := datastore.NewKey("a/b")
	bval := []byte("test")

	err = ds.Put(ctx, key, bval)
	require.NoError(err)

	val, err := ds.Get(ctx, key)
	require.NoError(err)

	require.Equal(bval, val)
}

func TestWriteBatch(t *testing.T) {
	require := require.New(t)

	dir, err := os.MkdirTemp("", "super_datastore")
	require.NoError(err)

	ds, err := NewDatastore(&DatastoreConfig{
		Folder:                dir,
		BlockCacheNumElements: 100,
		PackMaxNumElements:    1000,
	})
	require.NoError(err)

	ctx := context.Background()

	key := datastore.NewKey("a/b")
	bval := []byte("test")

	tx, err := ds.Batch(ctx)
	require.NoError(err)

	err = tx.Put(ctx, key, bval)
	require.NoError(err)

	err = tx.Put(ctx, key, bval)
	require.NoError(err)

	err = tx.Put(ctx, key, bval)
	require.NoError(err)

	err = tx.Commit(ctx)
	require.NoError(err)

	val, err := ds.Get(ctx, key)
	require.NoError(err)
	require.Equal(string(bval), string(val))

}
