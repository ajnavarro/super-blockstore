package packfile

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	ihash "github.com/ajnavarro/super-blockstore/hash"
	"github.com/ajnavarro/super-blockstore/idx"
	"github.com/ajnavarro/super-blockstore/iio"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
)

var ErrEntryNotFound = errors.New("entry not found")

// PackPack contains all the logic needed to get by key blocks from several packfiles.
// It will use indexes if available
type PackPack struct {
	path     string
	tempPath string

	packs *lru.Cache[string, *Reader]
	idx   *idx.MultiIndex
}

func NewPackPack(path, tempPath string, openedPacks int) (*PackPack, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(tempPath, 0755); err != nil {
		return nil, err
	}

	cache, err := lru.NewWithEvict(
		openedPacks,
		func(key string, value *Reader) {
			value.Close()
		},
	)
	if err != nil {
		return nil, err
	}

	i, err := idx.NewMulti(path, tempPath, openedPacks)
	if err != nil {
		return nil, err
	}

	pp := &PackPack{
		path:     path,
		tempPath: tempPath,
		packs:    cache,
		idx:      i,
	}

	return pp, nil
}

// TODO GetHashes

func (pp *PackPack) GetSize(key []byte) (uint32, error) {

	size, err := pp.idx.GetSize(ihash.SumBytes(key))
	if err != nil {
		return 0, err
	}

	// TODO: handle this better using coming error.
	if size == 0 {
		return 0, ErrEntryNotFound
	}

	return size, nil
}

func (pp *PackPack) Get(key []byte) ([]byte, error) {
	// TODO handle error not found
	packName, offset, err := pp.idx.GetOffset(ihash.SumBytes(key))
	if err == os.ErrNotExist {
		return nil, ErrEntryNotFound
	}

	if err != nil {
		return nil, err
	}

	pr, err := pp.getPack(packName)
	if err != nil {
		return nil, err
	}

	_, v, err := pr.ReadValueAt(offset)
	return v, err
}

func (pp *PackPack) Has(key []byte) (bool, error) {
	// TODO handle error not found
	_, _, err := pp.idx.GetOffset(ihash.SumBytes(key))
	if err == os.ErrNotExist {
		return false, ErrEntryNotFound
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (pp *PackPack) getPack(packName string) (*Reader, error) {
	pai, ok := pp.packs.Get(packName)
	if ok {
		return pai, nil
	}

	pr, err := NewPackFromFile(packPath(packName, pp.path))
	if err != nil {
		return nil, err
	}

	pp.packs.Add(packName, pr)

	return pr, nil
}

func (pp *PackPack) NewPackProcessing() (*PackProcessing, error) {

	packProc := &PackProcessing{
		tempPath:   pp.tempPath,
		packFolder: pp.path,
		idx:        pp.idx,
		pp:         pp,
	}
	return packProc, packProc.newPack()
}

func (pp *PackPack) Close() error {
	pp.packs.Purge()
	return pp.idx.Close()
}

type PackProcessing struct {
	tempPath   string
	packFolder string
	idx        idx.Idx

	processingPackID string

	txn idx.Transaction
	w   *Writer
	pp  *PackPack
}

func (pp *PackProcessing) closePack() error {
	if err := pp.w.Close(); err != nil {
		return err
	}

	if err := iio.Rename(
		packProcessingTxnPath(pp.processingPackID, pp.tempPath),
		packProcessingPath(pp.processingPackID, pp.tempPath),
	); err != nil {
		return err
	}

	return pp.txn.Commit()
}

func (pp *PackProcessing) newPack() error {
	packID := uuid.New().String()
	f, err := iio.OpenFile(packProcessingTxnPath(packID, pp.tempPath), os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	pp.processingPackID = packID

	txn, err := pp.idx.NewTransaction(packID)
	if err != nil {
		return err
	}

	pp.w = NewWriter(f)
	pp.txn = txn

	return pp.w.WriteHeader()
}

func (pp *PackProcessing) WriteBlock(key []byte, value []byte) error {
	size := uint32(len(value))

	pos, err := pp.w.WriteBlock(key, size, bytes.NewBuffer(value))
	if err != nil {
		return err
	}

	// TODO add CRC32
	if err := pp.txn.Add(ihash.SumBytes(key), 0, pos, size); err != nil {
		return err
	}

	return nil
}

func (pp *PackProcessing) Commit() error {
	if err := pp.closePack(); err != nil {
		return err
	}

	return iio.Rename(packProcessingPath(pp.processingPackID, pp.tempPath), packPath(pp.processingPackID, pp.packFolder))
}

func packPath(name, packPath string) string {
	return filepath.Join(packPath, fmt.Sprintf("%s.pack", name))

}

func packProcessingPath(name, packPath string) string {
	return filepath.Join(packPath, fmt.Sprintf("%s.pack.writting", name))
}

func packProcessingTxnPath(name, packPath string) string {
	return filepath.Join(packPath, fmt.Sprintf("txn-%s.pack.writting", name))
}
