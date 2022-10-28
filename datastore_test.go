package superblock

import (
	"context"
	"os"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/stretchr/testify/require"
)

// TODO create benchmarks comparing different datastores
func TestWriteAndReadPackfile(t *testing.T) {
	require := require.New(t)

	dir, err := os.MkdirTemp("", "super_datastore")
	require.NoError(err)

	ds, err := NewDatastore(&DatastoreConfig{
		Folder: dir,
	})
	require.NoError(err)

	ctx := context.Background()

	err = ds.Put(ctx, datastore.NewKey("a/b"), []byte("test"))
	require.NoError(err)
}
