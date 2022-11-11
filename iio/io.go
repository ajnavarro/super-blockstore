package iio

import (
	"os"
	"path"
)

// OpenFile opens a new file and creates all the directories if needed
func OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if err := os.MkdirAll(path.Dir(name), perm); err != nil {
		return nil, err
	}

	return os.OpenFile(name, flag, perm)
}

// WriteFile writes a new file and creates all the directories if needed
func WriteFile(name string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(path.Dir(name), perm); err != nil {
		return err
	}

	return os.WriteFile(name, data, perm)
}

// Rename moves the file and creates all the directories if needed
func Rename(from, to string) error {
	if err := os.MkdirAll(path.Dir(to), 0755); err != nil {
		return err
	}

	return os.Rename(from, to)
}
