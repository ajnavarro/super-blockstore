package superblock

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/iand/gonubs"
	"github.com/iand/gonudb"
	"github.com/ipfs/go-datastore"
	pebbleds "github.com/ipfs/go-ds-pebble"
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

	err = ds.Sync(ctx, datastore.NewKey("blah"))
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

	_, err = ds.Get(ctx, datastore.NewKey("non-existing"))
	require.Error(err)
	require.Equal(datastore.ErrNotFound, err)

}

var datastores = []struct {
	Name        string
	GetInstance func(path string) (datastore.Batching, error)
}{
	{
		Name: "gonudb",
		GetInstance: func(path string) (datastore.Batching, error) {
			return gonubs.NewDatastore(path, "gonudb-bench", &gonudb.StoreOptions{})
		},
	},
	{
		Name: "superBlockstore",
		GetInstance: func(path string) (datastore.Batching, error) {
			return NewDatastore(&DatastoreConfig{
				Folder:                path,
				BlockCacheNumElements: 1000,
				PackMaxNumElements:    1e7,
			})
		},
	},
	{
		Name: "pebble",
		GetInstance: func(path string) (datastore.Batching, error) {
			return pebbleds.NewDatastore(path, &pebble.Options{})
		},
	},

	// {
	// 	Name: "leveldb",
	// 	GetInstance: func(path string) (datastore.Batching, error) {
	// 		return leveldb.NewDatastore(path, nil)
	// 	},
	// },

	// {
	// 	Name: "flatfs",
	// 	GetInstance: func(path string) (datastore.Batching, error) {
	// 		return flatfs.CreateOrOpen(path, flatfs.Suffix(2), true)
	// 	},
	// },
	// {
	// 	Name: "badger3",
	// 	GetInstance: func(path string) (datastore.Batching, error) {
	// 		return badger3.NewDatastore(path, &badger3.DefaultOptions)
	// 	},
	// },
	// {
	// 	Name: "badger2",
	// 	GetInstance: func(path string) (datastore.Batching, error) {
	// 		return badger2.NewDatastore(path, &badger2.DefaultOptions)
	// 	},
	// },
	// {
	// 	Name: "badger1",
	// 	GetInstance: func(path string) (datastore.Batching, error) {
	// 		return badger.NewDatastore(path, &badger.DefaultOptions)
	// 	},
	// },
	// {
	// 	Name: "badger3-with-value-threshold-1KB",
	// 	GetInstance: func(path string) (datastore.Batching, error) {
	// 		opts := &badger3.DefaultOptions
	// 		opts.ValueThreshold = 1024
	// 		return badger3.NewDatastore(path, opts)
	// 	},
	// },
}

func genRandomBytes(len int) []byte {
	valueb := make([]byte, len)
	_, err := rand.Read(valueb)
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

// var numElementsPerBatch = []int{100, 1000, 10000, 100000, 1000000, 10000000, 100000000}
var numElementsPerBatch = []int{100, 1000, 10000, 100000}

var block = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
var biggerBlock = genRandomBytes(262144)

var values = [][]byte{block, biggerBlock}

func skipIf(dsName string, numElements int) (bool, string) {
	if dsName == "pebble" && numElements >= 100000000 {
		return true, "batch too large for pebble"
	}

	if dsName == "flatfs" && numElements >= 100000 {
		return true, "too many files for flatfs storage"
	}

	if dsName == "leveldb" && numElements >= 10000000 {
		return true, "too many files for lebeldb batches"

	}

	return false, ""
}

func BenchmarkCompareDatastores(b *testing.B) {
	for _, ds := range datastores {
		b.Run(fmt.Sprint("DS=", ds.Name), func(b *testing.B) {
			for _, value := range values {
				b.Run(fmt.Sprint("P=", len(value)), func(b *testing.B) {
					b.Run("ACTION=WriteOnBatch", func(b *testing.B) {
						require := require.New(b)

						dir, err := os.MkdirTemp("", fmt.Sprintf("%s-bench-write", ds.Name))
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

						b.ResetTimer()

						for i := 0; i < b.N; i++ {
							b.StopTimer()
							if skip, msg := skipIf(ds.Name, b.N); skip {
								b.Skip(msg)
							}

							key := datastore.NewKey(randKey(32))
							b.StartTimer()

							err = batch.Put(context.Background(), key, block)
							require.NoError(err)
						}

						err = batch.Commit(context.Background())
						require.NoError(err)
					})

					b.Run("ACTION=Write", func(b *testing.B) {
						require := require.New(b)

						dir, err := os.MkdirTemp("", fmt.Sprintf("%s-bench-write", ds.Name))
						require.NoError(err)
						b.Cleanup(func() {
							os.RemoveAll(dir)
						})

						store, err := ds.GetInstance(dir)
						require.NoError(err)
						b.Cleanup(func() {
							store.Close()
						})

						b.ResetTimer()

						for i := 0; i < b.N; i++ {
							b.StopTimer()
							if skip, msg := skipIf(ds.Name, b.N); skip {
								b.Skip(msg)
							}

							key := datastore.NewKey(randKey(32))
							b.StartTimer()

							err = store.Put(context.Background(), key, block)
							require.NoError(err)
						}
					})

					for _, nepb := range numElementsPerBatch {
						b.Run(fmt.Sprint("N=", nepb), func(b *testing.B) {
							if skip, msg := skipIf(ds.Name, nepb); skip {
								b.Skip(msg)
							}

							dir, err := os.MkdirTemp("", fmt.Sprintf("%s-bench-read", ds.Name))
							require.NoError(b, err)

							b.Cleanup(func() {
								os.RemoveAll(dir)
							})

							var eventlyDistributedSamples []datastore.Key
							var consecutiveSamples []datastore.Key

							store, err := ds.GetInstance(dir)
							require.NoError(b, err)

							batch, err := store.Batch(context.Background())
							require.NoError(b, err)

							for i := 0; i < nepb; i++ {
								b.StopTimer()

								key := datastore.NewKey(randKey(32))

								if i%(nepb/100) == 0 {
									eventlyDistributedSamples = append(eventlyDistributedSamples, key)
								}

								if i >= nepb-100 && i < nepb {
									consecutiveSamples = append(consecutiveSamples, key)
								}

								err = batch.Put(context.Background(), key, block)
								require.NoError(b, err)
							}

							err = batch.Commit(context.Background())
							require.NoError(b, err)

							err = store.Close()
							require.NoError(b, err)

							b.Run("ACTION=DiskUsage", func(b *testing.B) {
								store, err := ds.GetInstance(dir)
								require.NoError(b, err)

								b.ResetTimer()

								for i := 0; i < b.N; i++ {
									usage, err := datastore.DiskUsage(context.Background(), store)
									require.NoError(b, err)

									b.StopTimer()
									b.ReportMetric(float64(usage), "disk")
									b.StartTimer()
								}

								err = store.Close()
								require.NoError(b, err)
							})

							b.Run("ACTION=Read100EventlyDistributed", func(b *testing.B) {
								store, err := ds.GetInstance(dir)
								require.NoError(b, err)
								b.Cleanup(func() {
									store.Close()
								})

								b.ResetTimer()

								for _, n := range []int{1, 2, 3} {
									b.Run(fmt.Sprintf("ITER=%d", n), func(b *testing.B) {
										for i := 0; i < b.N; i++ {
											for _, k := range eventlyDistributedSamples {
												v, err := store.Get(context.Background(), k)
												require.NoError(b, err)
												require.Equal(b, block, v)
											}
										}
									})
								}

							})

							b.Run("ACTION=Read100ConsecutiveKeys", func(b *testing.B) {
								store, err := ds.GetInstance(dir)
								require.NoError(b, err)
								b.Cleanup(func() {
									store.Close()
								})

								b.ResetTimer()

								for _, n := range []int{1, 2, 3} {
									b.Run(fmt.Sprintf("ITER=%d", n), func(b *testing.B) {
										for i := 0; i < b.N; i++ {
											for _, k := range consecutiveSamples {
												v, err := store.Get(context.Background(), k)
												require.NoError(b, err)
												require.Equal(b, block, v)
											}
										}
									})
								}
							})

							b.Run("ACTION=NonExistingKey", func(b *testing.B) {
								store, err := ds.GetInstance(dir)
								require.NoError(b, err)
								b.Cleanup(func() {
									store.Close()
								})

								key := datastore.NewKey(randKey(32))

								b.ResetTimer()

								for _, n := range []int{1, 2, 3} {
									b.Run(fmt.Sprintf("ITER=%d", n), func(b *testing.B) {
										for i := 0; i < b.N; i++ {
											_, err := store.Get(context.Background(), key)
											require.ErrorIs(b, err, datastore.ErrNotFound)
										}
									})
								}
							})
						})
					}
				})
			}
		})
	}
}
