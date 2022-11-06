package storage

import (
	"encoding/hex"
	"errors"
	"os"
	"path"
	"path/filepath"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
)

type ObjectStorage struct {
	path string
}

func NewObjectStorage(path string) *ObjectStorage {
	return &ObjectStorage{path: path}
}

func (s *ObjectStorage) Add(key ihash.Hash, value []byte) error {
	fp := s.filePath(key)
	dir := path.Dir(fp)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return os.WriteFile(fp, value, 0755)
}

func (s *ObjectStorage) Del(key ihash.Hash) error {
	err := os.Remove(s.filePath(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	return err
}

func (s *ObjectStorage) Flush() error {
	// TODO implement and keep some objects into memory before flushing
	return nil
}

func (s *ObjectStorage) Get(key ihash.Hash) ([]byte, error) {
	return os.ReadFile(s.filePath(key))
}

func (s *ObjectStorage) Has(key ihash.Hash) (bool, error) {
	_, err := os.Stat(s.filePath(key))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err == nil {
		return true, nil
	}

	return false, err
}

func (s *ObjectStorage) DeleteAll() error {
	return os.RemoveAll(s.path)
}

func (s *ObjectStorage) GetAll() (*Iterator, error) {
	file, err := os.OpenFile(s.path, os.O_RDONLY, 0755)

	if err != nil {
		return nil, err
	}

	return &Iterator{file: file}, err
}

func (s *ObjectStorage) filePath(key ihash.Hash) string {
	name := hex.EncodeToString(key[:])
	return path.Join(s.path, name[0:2], name)
}

type Iterator struct {
	file   *os.File
	dirPos int

	reading []os.DirEntry
}

func (i *Iterator) Next() ([]byte, []byte, error) {
	if len(i.reading) == 0 {
		// TODO load reading
	}

	de, reading := i.reading[0], i.reading[1:]
	i.reading = reading

	fn := filepath.Base(de.Name())
	key, err := hex.DecodeString(fn)
	if err != nil {
		return nil, nil, err
	}

	value, err := os.ReadFile(de.Name())

	return key, value, err
}
