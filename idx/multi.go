package idx

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	ihash "github.com/ajnavarro/super-blockstore/hash"
	lru "github.com/hashicorp/golang-lru/v2"
)

var _ Idx = &MultiIndex{}

type MultiIndex struct {
	indexes        *lru.Cache[string, *IndexReader]
	path           string
	processingPath string

	mu  sync.RWMutex
	ids map[string]struct{}
}

func NewMulti(path, processingPath string, maxOpenIndexes int) (*MultiIndex, error) {
	cache, err := lru.New[string, *IndexReader](
		maxOpenIndexes,
	)
	if err != nil {
		return nil, err
	}

	mi := &MultiIndex{
		indexes:        cache,
		path:           path,
		processingPath: processingPath,
		ids:            map[string]struct{}{},
	}

	return mi, mi.reloadPacks()
}

func (i *MultiIndex) lookup(irfs func(string, *IndexReader) error) error {
	i.mu.RLock()
	defer i.mu.RUnlock()
	var forLater []string
	for k := range i.ids {
		ir, ok := i.indexes.Get(k)
		if !ok {
			forLater = append(forLater, k)
			continue
		}

		err := irfs(k, ir)
		if err == ErrEntryNotFound {
			continue
		}
		if err != nil {
			return err
		}

		return nil
	}

	// After searching on cached indexes, we need to check uncached ones:
	for _, k := range forLater {
		ir, err := NewIndexFromFile(indexPath(k, i.path))
		if err != nil {
			return err
		}

		err = irfs(k, ir)
		if err == ErrEntryNotFound {
			continue
		}
		if err != nil {
			return err
		}

		// only add to LRU cache if we find something
		i.indexes.Add(k, ir)

		return nil
	}

	return ErrEntryNotFound
}

func (i *MultiIndex) GetOffset(key ihash.Hash) (string, int64, error) {
	var packID string
	var offset int64
	err := i.lookup(func(id string, ir *IndexReader) error {
		off, err := ir.GetOffset(key)
		offset = off
		packID = id
		return err
	})

	return packID, offset, err
}

func (i *MultiIndex) Contains(key ihash.Hash) (bool, error) {
	var contains bool
	err := i.lookup(func(id string, ir *IndexReader) error {
		ok, err := ir.Contains(key)
		contains = ok
		return err
	})

	return contains, err
}

func (i *MultiIndex) GetSize(key ihash.Hash) (uint32, error) {
	var size uint32
	err := i.lookup(func(id string, ir *IndexReader) error {
		s, err := ir.GetSize(key)
		size = s
		return err
	})

	return size, err
}

func (i *MultiIndex) DeleteAll(packName string) error {
	if err := os.Remove(indexPath(packName, i.path)); err != nil {
		return err
	}

	delete(i.ids, packName)

	i.indexes.Remove(packName)

	return nil
}

func (i *MultiIndex) NewTransaction(packName string) (Transaction, error) {
	return &multiIndexTransaction{
		w:              NewIndexWriter(),
		packName:       packName,
		path:           i.path,
		processingPath: i.processingPath,
		ids:            i.ids,
	}, nil
}

func (i *MultiIndex) Close() error {
	i.ids = nil
	i.indexes.Purge()

	return nil
}

type multiIndexTransaction struct {
	w *IndexWriter

	packName       string
	path           string
	processingPath string

	ids map[string]struct{}
}

func (txn *multiIndexTransaction) Add(key ihash.Hash, crc32 uint32, pos int64, size uint32) error {
	txn.w.AddRaw(key, crc32, uint64(pos), size)
	return nil
}

func (txn *multiIndexTransaction) Commit() error {
	pp := indexProcessingPath(txn.packName, txn.processingPath)
	if err := WriteIndex(txn.w, pp); err != nil {
		return err
	}

	ip := indexPath(txn.packName, txn.path)

	if err := os.Rename(pp, ip); err != nil {
		return err
	}

	txn.ids[txn.packName] = struct{}{}

	return nil
}

func (txn *multiIndexTransaction) Discard() error {
	txn.w = nil
	return nil
}

func (i *MultiIndex) reloadPacks() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return filepath.WalkDir(i.path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		_, file := path.Split(p)
		ext := path.Ext(file)
		key := strings.TrimSuffix(file, ext)

		if ext == ".idx" {
			if _, exists := i.ids[key]; exists {
				return nil
			}

			i.ids[key] = struct{}{}
		}

		return nil
	})
}

func indexPath(name, packPath string) string {
	return path.Join(packPath, fmt.Sprintf("%s.idx", name))
}

func indexProcessingPath(name, packPath string) string {
	return path.Join(packPath, fmt.Sprintf("%s.idx.writting", name))
}
