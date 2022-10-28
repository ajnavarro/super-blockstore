package superblock

import (
	"context"
	"errors"
	"os"
	"path"

	"github.com/ajnavarro/super-blockstorage/packfile"
	"github.com/ajnavarro/super-blockstorage/storage"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

var _ datastore.Datastore = &Datastore{}
var _ datastore.Batching = &Datastore{}
var _ datastore.CheckedDatastore = &Datastore{}
var _ datastore.GCDatastore = &Datastore{}
var _ datastore.PersistentDatastore = &Datastore{}
var _ datastore.TxnDatastore = &Datastore{}

// TODO key/position indexes
// TODO packfiles will be created by key path for easier querying
// TODO problem: delete and after that add the same block with no GC on the middle. Tombstone will have the hash, so when executing GC we will consider it as deleted.
//   - Maybe add sharding instead of using simple packfiles
type DatastoreConfig struct {
	Folder string

	BlockCacheNumElements int
}

const objectFolder = "objects"
const packFolder = "packs"

const tombstoneName = "tombstone.bin"

type Datastore struct {
	ts *packfile.Tombstone
	bc *lru.Cache
	os *storage.ObjectStorage
	pp *packfile.PackPack
	// TODO pool reader?
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
	pp := packfile.NewPackPack(path.Join(cfg.Folder, packFolder))
	return &Datastore{
		ts: ts,
		bc: lcache,
		os: os,
		pp: pp,
	}, nil
}

func (ds *Datastore) NewTransaction(ctx context.Context, readOnly bool) (datastore.Txn, error) {
	panic("not implemented") // TODO: Implement
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
	deleted, err := ds.ts.Has(key.Bytes())
	if err != nil {
		return nil, err
	}

	if deleted {
		return nil, datastore.ErrNotFound
	}

	vali, ok := ds.bc.Get(key.Bytes())
	if ok {
		return vali.([]byte), nil
	}

	val, err := ds.os.Get(key.Bytes())
	if err != nil {
		return nil, err
	}

	if val != nil {
		return val, nil
	}

	val, err = ds.pp.Get(key.Bytes())
	if errors.Is(err, os.ErrNotExist) {
		return nil, datastore.ErrNotFound
	}

	ds.bc.Add(key.Bytes(), val)

	return val, err
}

// Has returns whether the `key` is mapped to a `value`.
// In some contexts, it may be much cheaper only to check for existence of
// a value, rather than retrieving the value itself. (e.g. HTTP HEAD).
// The default implementation is found in `GetBackedHas`.
func (ds *Datastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	if ds.bc.Contains(key.Bytes()) {
		return true, nil
	}

	deleted, err := ds.ts.Has(key.Bytes())
	if err != nil {
		return false, err
	}

	if deleted {
		return false, nil
	}

	contains, err := ds.os.Has(key.Bytes())
	if err != nil {
		return false, err
	}

	if contains {
		return true, nil
	}

	return ds.pp.Has(key.Bytes())
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
	// TODO avoid duplicated values???
	return ds.os.Add(key.Bytes(), value)
}

// Delete removes the value for given `key`. If the key is not in the
// datastore, this method returns no error.
func (ds *Datastore) Delete(ctx context.Context, key datastore.Key) error {
	ds.bc.Remove(key.Bytes())
	return ds.ts.AddKey(key.Bytes())
}

// Sync guarantees that any Put or Delete calls under prefix that returned
// before Sync(prefix) was called will be observed after Sync(prefix)
// returns, even if the program crashes. If Put/Delete operations already
// satisfy these requirements then Sync may be a no-op.
//
// If the prefix fails to Sync this method returns an error.
func (ds *Datastore) Sync(ctx context.Context, prefix datastore.Key) error {
	return ds.os.Flush()
}

func (ds *Datastore) Close() error {
	// TODO add more closes
	return ds.ts.Close()
}

func (ds *Datastore) Batch(ctx context.Context) (datastore.Batch, error) {
	panic("not implemented") // TODO: Implement
}

func (ds *Datastore) Check(ctx context.Context) error {
	panic("not implemented") // TODO: Implement
}
