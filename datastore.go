package superblock

import (
	"context"
	"errors"
	"os"
	"path"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
	"github.com/ajnavarro/super-blockstorage/packfile"
	"github.com/ajnavarro/super-blockstorage/storage"
)

var _ datastore.Datastore = &Datastore{}
var _ datastore.Batching = &Datastore{}
var _ datastore.CheckedDatastore = &Datastore{}
var _ datastore.GCDatastore = &Datastore{}
var _ datastore.PersistentDatastore = &Datastore{}

type DatastoreConfig struct {
	Folder string

	BlockCacheNumElements int
}

const objectFolder = "objects"
const packFolder = "packs"
const processingFolder = "processing"

const tombstoneName = "tombstone.bin"

type Datastore struct {
	ts    *packfile.Tombstone
	cache *lru.Cache
	os    *storage.ObjectStorage
	pp    *PackPack

	path string
}

func NewDatastore(cfg *DatastoreConfig) (*Datastore, error) {
	ts, err := packfile.NewTombstonePath(path.Join(cfg.Folder, tombstoneName))
	if err != nil {
		return nil, err
	}

	lcache, err := lru.New(cfg.BlockCacheNumElements)
	if err != nil {
		return nil, err
	}

	os := storage.NewObjectStorage(path.Join(cfg.Folder, objectFolder))
	pp := NewPackPack(cfg.Folder)

	return &Datastore{
		path:  cfg.Folder,
		ts:    ts,
		cache: lcache,
		os:    os,
		pp:    pp,
	}, nil
}

// DiskUsage returns the space used by a datastore, in bytes.
func (ds *Datastore) DiskUsage(ctx context.Context) (uint64, error) {
	// TODO get stats from packfiles, indexes and the thombstone. Get also stats from pending files.
	panic("not implemented") // TODO: Implement
}

func (ds *Datastore) CollectGarbage(ctx context.Context) error {
	// TODO create packfiles using the specified size
	// TODO start reading objects from ObjectStorage
	// TODO non blocking GC on background. Rename a file is an atomic operation.
	// TODO check previous GC attempt and delete pending objects

	panic("not implemented") // TODO: Implement
}

// Get retrieves the object `value` named by `key`.
// Get will return ErrNotFound if the key is not mapped to a value.
func (ds *Datastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	k := ihash.SumBytes(key.Bytes())

	deleted, err := ds.ts.HasHash(k)
	if err != nil {
		return nil, err
	}

	if deleted {
		return nil, datastore.ErrNotFound
	}

	vali, ok := ds.cache.Get(k)
	if ok {
		return vali.([]byte), nil
	}

	val, err := ds.os.Get(k)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if val != nil {
		return val, nil
	}

	val, err = ds.pp.GetHash(k)
	if errors.Is(err, os.ErrNotExist) {
		return nil, datastore.ErrNotFound
	}

	if err != nil {
		return nil, err
	}

	ds.cache.Add(k, val)

	return val, err
}

// Has returns whether the `key` is mapped to a `value`.
// In some contexts, it may be much cheaper only to check for existence of
// a value, rather than retrieving the value itself. (e.g. HTTP HEAD).
// The default implementation is found in `GetBackedHas`.
func (ds *Datastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	k := ihash.SumBytes(key.Bytes())

	if ds.cache.Contains(k) {
		return true, nil
	}

	deleted, err := ds.ts.HasHash(k)
	if err != nil {
		return false, err
	}

	if deleted {
		return false, nil
	}

	contains, err := ds.os.Has(k)
	if err != nil {
		return false, err
	}

	if contains {
		return true, nil
	}

	return ds.pp.HasHash(k)
}

// GetSize returns the size of the `value` named by `key`.
// In some contexts, it may be much cheaper to only get the size of the
// value rather than retrieving the value itself.
func (ds *Datastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	// TODO naive GetSize implementation. Improve.
	val, err := ds.Get(ctx, key)
	return len(val), err
}

// Query searches the datastore and returns a query result. This function
// may return before the query actually runs. To wait for the query:
//
//   result, _ := ds.Query(q)
//
//   // use the channel interface; result may come in at different times
//   for entry := range result.Next() { ... }
//
//   // or wait for the query to be completely done
//   entries, _ := result.Rest()
//   for entry := range entries { ... }
//
func (ds *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	panic("not implemented") // TODO: Implement
}

// Put stores the object `value` named by `key`.
//
// The generalized Datastore interface does not impose a value type,
// allowing various datastore middleware implementations (which do not
// handle the values directly) to be composed together.
//
// Ultimately, the lowest-level datastore will need to do some value checking
// or risk getting incorrect values. It may also be useful to expose a more
// type-safe interface to your application, and do the checking up-front.
func (ds *Datastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	k := ihash.SumBytes(key.Bytes())

	return ds.os.Add(k, value)
}

// Delete removes the value for given `key`. If the key is not in the
// datastore, this method returns no error.
func (ds *Datastore) Delete(ctx context.Context, key datastore.Key) error {
	k := ihash.SumBytes(key.Bytes())
	ds.cache.Remove(k)
	return ds.ts.AddHash(k)
}

// Sync guarantees that any Put or Delete calls under prefix that returned
// before Sync(prefix) was called will be observed after Sync(prefix)
// returns, even if the program crashes. If Put/Delete operations already
// satisfy these requirements then Sync may be a no-op.
//
// If the prefix fails to Sync this method returns an error.
func (ds *Datastore) Sync(ctx context.Context, prefix datastore.Key) error {
	panic("not implemented") // TODO: Implement
}

func (ds *Datastore) Close() error {
	// TODO add more closes
	return ds.ts.Close()
}

func (ds *Datastore) Batch(ctx context.Context) (datastore.Batch, error) {
	return NewBatch(ds.path, ds.pp)
}

func (ds *Datastore) Check(ctx context.Context) error {
	panic("not implemented") // TODO: Implement
}
