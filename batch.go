package superblock

import (
	"context"
	"errors"

	"github.com/ipfs/go-datastore"

	"github.com/ajnavarro/super-blockstorage/packfile"
)

var ErrNotSupportedOnBatches = errors.New("this method is not supported on batches")

var _ datastore.Batch = &Batch{}

type Batch struct {
	packProc *packfile.PackProcessing
}

func NewBatch(packProcessing *packfile.PackProcessing) *Batch {
	return &Batch{
		packProc: packProcessing,
	}
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
func (tx *Batch) Put(ctx context.Context, key datastore.Key, value []byte) error {
	return tx.packProc.WriteBlock(key.Bytes(), value)
}

// Delete removes the value for given `key`. If the key is not in the
// datastore, this method returns no error.
func (tx *Batch) Delete(ctx context.Context, key datastore.Key) error {
	return ErrNotSupportedOnBatches
}

// Commit finalizes a transaction, attempting to commit it to the Datastore.
// May return an error if the transaction has gone stale. The presence of an
// error is an indication that the data was not committed to the Datastore.
func (tx *Batch) Commit(ctx context.Context) error {
	return tx.packProc.Commit()
}
