package superblock

import (
	"context"
	"errors"
	"io/fs"
	"path"
	"path/filepath"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"go.uber.org/multierr"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
	"github.com/ajnavarro/super-blockstorage/packfile"
)

var _ datastore.Datastore = &Datastore{}
var _ datastore.Batching = &Datastore{}
var _ datastore.CheckedDatastore = &Datastore{}
var _ datastore.GCDatastore = &Datastore{}
var _ datastore.PersistentDatastore = &Datastore{}

const packFolder = "packs"
const processingFolder = "processing"

const tombstoneName = "tombstone.bin"

type Datastore struct {
	ts    *packfile.Tombstone
	cache *lru.Cache[ihash.Hash, []byte]
	pp    *packfile.PackPack

	mu            sync.Mutex // protects singleObjects
	singleObjects *packfile.PackProcessing

	folder          string
	elementsPerPack int
}

func NewDatastore(cfg *DatastoreConfig) (*Datastore, error) {
	cfg.FillDefaults()

	ts, err := packfile.NewTombstonePath(path.Join(cfg.Folder, tombstoneName))
	if err != nil {
		return nil, err
	}

	lcache, err := lru.New[ihash.Hash, []byte](cfg.BlockCacheNumElements)
	if err != nil {
		return nil, err
	}

	processingFolder := path.Join(cfg.Folder, processingFolder)

	ppf := path.Join(cfg.Folder, packFolder)
	pp, err := packfile.NewPackPack(ppf, processingFolder, cfg.MaxOpenPacks)
	if err != nil {
		return nil, err
	}

	packProcessing, err := pp.NewPackProcessing()
	if err != nil {
		return nil, err
	}

	// TODO check previous GC attempt and delete pending objects

	return &Datastore{
		ts:    ts,
		cache: lcache,
		pp:    pp,

		singleObjects: packProcessing,

		folder:          cfg.Folder,
		elementsPerPack: cfg.PackMaxNumElements,
	}, nil
}

// DiskUsage returns the space used by a datastore, in bytes.
func (ds *Datastore) DiskUsage(ctx context.Context) (uint64, error) {
	var size uint64
	err := filepath.WalkDir(ds.folder, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		i, err := d.Info()
		if err != nil {
			return err
		}

		size += uint64(i.Size())

		return nil
	})

	return size, err
}

func (ds *Datastore) CollectGarbage(ctx context.Context) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	// first, we pack objects from objectStorage
	if err := ds.singleObjects.Commit(); err != nil {
		return err
	}

	// TODO repack previous packfiles
	// TODO implement a heavy GC, to read using the indexes and remove possible duplicated blocks on packfiles
	// TODO create MIDXs

	return nil
}

// Get retrieves the object `value` named by `key`.
// Get will return ErrNotFound if the key is not mapped to a value.
func (ds *Datastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	k := ihash.SumBytes(key.Bytes())

	vali, ok := ds.cache.Get(k)
	if ok {
		return vali, nil
	}

	deleted, err := ds.ts.HasHash(k)
	if err != nil {
		return nil, err
	}

	if deleted {
		return nil, datastore.ErrNotFound
	}

	val, err := ds.pp.Get(key.Bytes())
	if errors.Is(err, packfile.ErrEntryNotFound) {
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

	return ds.pp.Has(key.Bytes())
}

// GetSize returns the size of the `value` named by `key`.
// In some contexts, it may be much cheaper to only get the size of the
// value rather than retrieving the value itself.
func (ds *Datastore) GetSize(ctx context.Context, key datastore.Key) (int, error) {
	size, err := ds.pp.GetSize(key.Bytes())
	if err == packfile.ErrEntryNotFound {
		return 0, datastore.ErrNotFound
	}

	return int(size), err

}

// Query searches the datastore and returns a query result. This function
// may return before the query actually runs. To wait for the query:
//
//	result, _ := ds.Query(q)
//
//	// use the channel interface; result may come in at different times
//	for entry := range result.Next() { ... }
//
//	// or wait for the query to be completely done
//	entries, _ := result.Rest()
//	for entry := range entries { ... }
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
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.singleObjects.WriteBlock(key.Bytes(), value)
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
	ds.mu.Lock()
	defer ds.mu.Unlock()

	return ds.singleObjects.Commit()
}

func (ds *Datastore) Close() error {
	ds.cache.Purge()

	return multierr.Combine(
		ds.pp.Close(),
		ds.ts.Close(),
	)
}

func (ds *Datastore) Batch(ctx context.Context) (datastore.Batch, error) {
	pp, err := ds.pp.NewPackProcessing()
	if err != nil {
		return nil, err
	}

	return NewBatch(pp), nil
}

func (ds *Datastore) Check(ctx context.Context) error {
	panic("not implemented") // TODO: Implement
}
