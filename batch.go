package superblock

import (
	"bytes"
	"context"
	"errors"

	"github.com/ipfs/go-datastore"

	"github.com/ajnavarro/super-blockstorage/iio"
	"github.com/ajnavarro/super-blockstorage/packfile"
)

var ErrNotSupportedOnTransactions = errors.New("this method is not supported on transactions")

var _ datastore.Batch = &Batch{}

type Batch struct {
	idx *packfile.Index
	pw  *packfile.Writer
	pp  *PackPack

	path         string
	packfilePath string
}

func NewBatch(p string, pp *PackPack) (*Batch, error) {
	pw, pn, err := NewPackProcessing(p)
	if err != nil {
		return nil, err
	}

	return &Batch{
		idx:          packfile.NewIndex(),
		pw:           pw,
		pp:           pp,
		path:         p,
		packfilePath: pn,
	}, nil
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
	pos, err := tx.pw.WriteBlock(key.Bytes(), int64(len(value)), bytes.NewReader(value))
	if err != nil {
		return err
	}

	tx.idx.Add(key.Bytes(), pos)

	return nil
}

// Delete removes the value for given `key`. If the key is not in the
// datastore, this method returns no error.
func (tx *Batch) Delete(ctx context.Context, key datastore.Key) error {
	return ErrNotSupportedOnTransactions
}

// Commit finalizes a transaction, attempting to commit it to the Datastore.
// May return an error if the transaction has gone stale. The presence of an
// error is an indication that the data was not committed to the Datastore.
func (tx *Batch) Commit(ctx context.Context) error {
	name := tx.pw.Hash()

	if err := tx.pw.Close(); err != nil {
		return err
	}

	if err := iio.Rename(tx.packfilePath, PackPath(name, tx.path)); err != nil {
		return err
	}

	if err := packfile.WriteIndexAtomically(tx.idx, IndexProcessingPath(name, tx.path), IndexPath(name, tx.path)); err != nil {
		return err
	}

	return tx.pp.AddPack(name)
}
