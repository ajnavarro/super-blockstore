package storage

import (
	"encoding/hex"
	"errors"
	"os"
	"path"
	"path/filepath"
)

type ObjectStorage struct {
	path string
}

func NewObjectStorage(path string) *ObjectStorage {
	return &ObjectStorage{path: path}
}

func (s *ObjectStorage) Add(key, value []byte) error {
	return os.WriteFile(s.filePath(key), value, 0755)
}

func (s *ObjectStorage) Del(key []byte) error {
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

func (s *ObjectStorage) Get(key []byte) ([]byte, error) {
	return os.ReadFile(s.filePath(key))
}

func (s *ObjectStorage) Has(key []byte) (bool, error) {
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

func (s *ObjectStorage) filePath(key []byte) string {
	name := hex.EncodeToString(key)
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
