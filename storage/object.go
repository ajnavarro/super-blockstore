package storage

import (
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sync"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
	"github.com/ajnavarro/super-blockstorage/iio"
)

type ObjectStorage struct {
	mu           sync.RWMutex // mu protects a deleteAll action from readers
	path         string
	temporalPath string
}

func NewObjectStorage(path, temporalPath string) *ObjectStorage {
	return &ObjectStorage{
		path:         path,
		temporalPath: temporalPath,
	}
}

func (s *ObjectStorage) Add(key ihash.Hash, value []byte) error {
	ftp := filePath(s.temporalPath, key)
	if err := iio.WriteFile(ftp, value, 0755); err != nil {
		return err
	}

	fp := filePath(s.path, key)

	return iio.Rename(ftp, fp)
}

func (s *ObjectStorage) Del(key ihash.Hash) error {
	err := os.Remove(filePath(s.path, key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	return err
}

func (s *ObjectStorage) Get(key ihash.Hash) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return os.ReadFile(filePath(s.path, key))
}

func (s *ObjectStorage) Has(key ihash.Hash) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, err := os.Stat(filePath(s.path, key))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err == nil {
		return true, nil
	}

	return false, err
}

func (s *ObjectStorage) DeleteAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return os.RemoveAll(s.path)
}

func (s *ObjectStorage) GetAll() (*Iterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return newIterator(s.path)
}

func filePath(base string, key ihash.Hash) string {
	name := hex.EncodeToString(key[:])
	return path.Join(base, name[0:2], name)
}

type Iterator struct {
	reading []string
}

func newIterator(folder string) (*Iterator, error) {
	var entries []string
	err := filepath.WalkDir(folder, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		entries = append(entries, p)

		return nil
	})

	return &Iterator{reading: entries}, err
}

func (i *Iterator) Next() (ihash.Hash, []byte, error) {
	if len(i.reading) == 0 {
		return ihash.Hash{}, nil, io.EOF
	}

	de, reading := i.reading[0], i.reading[1:]
	i.reading = reading

	pn := path.Base(de)
	key, err := hex.DecodeString(pn)
	if err != nil {
		return ihash.Hash{}, nil, err
	}

	var keyOut ihash.Hash
	copy(keyOut[:], key)

	value, err := os.ReadFile(de)

	return keyOut, value, err
}
