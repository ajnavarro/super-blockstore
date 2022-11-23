package superblock

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"

	"github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	badger2 "github.com/ipfs/go-ds-badger2"
	flatfs "github.com/ipfs/go-ds-flatfs"
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

type datastoreInterface interface {
	datastore.Datastore
	datastore.Batching
}

var datastores = []struct {
	Name        string
	GetInstance func(path string) (datastoreInterface, error)
}{
	// TODO it does not implement the correct batch API
	// {
	// 	Name: "pebble",
	// 	GetInstance: func(path string) (datastoreInterface, error) {
	// 		return pebbleds.NewDatastore(path)
	// 	},
	// },
	{
		Name: "flatfs",
		GetInstance: func(path string) (datastoreInterface, error) {
			return flatfs.CreateOrOpen(path, flatfs.Suffix(2), true)
		},
	},
	{
		Name: "superBlockstore",
		GetInstance: func(path string) (datastoreInterface, error) {
			return NewDatastore(&DatastoreConfig{
				Folder:                path,
				BlockCacheNumElements: 1000,
				PackMaxNumElements:    1e6,
			})
		},
	},
	{
		Name: "badger1",
		GetInstance: func(path string) (datastoreInterface, error) {
			return badger.NewDatastore(path, &badger.DefaultOptions)
		},
	},
	{
		Name: "badger2",
		GetInstance: func(path string) (datastoreInterface, error) {
			return badger2.NewDatastore(path, &badger2.DefaultOptions)
		},
	},
	// it fails with OOM with big batches.
	// {
	// 	Name: "leveldb",
	// 	GetInstance: func(path string) (datastoreInterface, error) {
	// 		return leveldb.NewDatastore(path, nil)
	// 	},
	// },
}

func genRandomBytes() []byte {
	var valueb [262144]byte
	_, err := rand.Read(valueb[:])
	if err != nil {
		panic(err)
	}

	return valueb[:]
}

var letters = []rune("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randKey(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

var numElementsPerBatch = []int{100, 1000, 10000, 100000, 1000000, 10000000, 100000000}
var block = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
var biggerBlock = genRandomBytes()
var values = [][]byte{block, biggerBlock}

func BenchmarkCompareDatastores(b *testing.B) {
	for _, ds := range datastores {
		b.Run(ds.Name, func(b *testing.B) {
			for _, nepb := range numElementsPerBatch {
				b.Run(fmt.Sprint("N=", nepb), func(b *testing.B) {
					for _, value := range values {
						b.Run(fmt.Sprint("P=", len(value)), func(b *testing.B) {
							require := require.New(b)

							dir, err := os.MkdirTemp("", fmt.Sprintf("%s-bench", ds.Name))
							require.NoError(err)
							b.Cleanup(func() {
								os.RemoveAll(dir)
							})

							store, err := ds.GetInstance(dir)
							require.NoError(err)
							b.Cleanup(func() {
								store.Close()
							})

							batch, err := store.Batch(context.Background())
							require.NoError(err)

							var samples []datastore.Key
							for i := 0; i < nepb; i++ {

								key := datastore.NewKey(randKey(32))

								if i%(nepb/100) == 0 {
									samples = append(samples, key)
								}

								err = batch.Put(context.Background(), key, block)
								require.NoError(err)
							}

							err = batch.Commit(context.Background())
							require.NoError(err)

							runtime.GC()

							b.ResetTimer()

							for i := 0; i < b.N; i++ {
								for _, k := range samples {
									v, err := store.Get(context.Background(), k)
									require.NoError(err)
									require.Equal(block, v)
								}
							}

							var m runtime.MemStats
							runtime.ReadMemStats(&m)
							b.ReportMetric(float64(m.Alloc), "total_mem_alloc")
						})
					}
				})
			}
		})
	}
}
