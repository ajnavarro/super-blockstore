package packfile

import (
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
	"github.com/ajnavarro/super-blockstorage/iio"
)

// PackPack contains all the logic needed to get by key blocks from several packfiles.
// It will use indexes if available
type PackPack struct {
	path     string
	tempPath string

	mu    sync.RWMutex
	packs map[string]*packAndIndex
}

type packAndIndex struct {
	idx *IndexReader
	pr  *ReaderSnappy
}

func NewPackPack(path, tempPath string) (*PackPack, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(tempPath, 0755); err != nil {
		return nil, err
	}

	pp := &PackPack{
		path:     path,
		tempPath: tempPath,
		packs:    make(map[string]*packAndIndex),
	}

	return pp, pp.reloadPacks()
}

// TODO GetHashes

func (pp *PackPack) GetSize(key ihash.Hash) (int64, error) {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	for _, ip := range pp.packs {
		entry, err := ip.idx.GetRaw(key)
		if err != nil {
			return 0, err
		}

		if entry == nil {
			continue
		}

		return entry.Size, nil
	}

	return 0, os.ErrNotExist
}

func (pp *PackPack) Get(key ihash.Hash) ([]byte, error) {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	// TODO improve
	result := make(chan []byte)
	errChan := make(chan error)
	var wg sync.WaitGroup
	for _, ip := range pp.packs {
		wg.Add(1)
		go func(ip *packAndIndex) {
			defer wg.Done()

			entry, err := ip.idx.GetRaw(key)
			if err != nil {
				errChan <- err
				return
			}

			if entry == nil {
				result <- nil
				return
			}
			_, v, err := ip.pr.ReadValueAt(entry.Offset)
			if err != nil {
				errChan <- err
				return
			}

			result <- v
		}(ip)
	}

	doneCh := make(chan bool)
	go func() {
		wg.Wait()
		doneCh <- true
		close(errChan)
		close(result)
		close(doneCh)
	}()

	var outErr error
	var outVal []byte

loop:
	for {
		select {
		case err, ok := <-errChan:
			if !ok {
				break loop
			}

			outErr = err

			break loop
		case v, ok := <-result:
			if !ok {
				break loop
			}

			if v == nil {
				continue loop
			}

			outVal = v

			break loop
		case <-doneCh:
			break loop
		}
	}

	if outErr != nil {
		return nil, outErr
	}

	if outVal == nil {
		return nil, os.ErrNotExist
	}

	return outVal, nil
}

func (pp *PackPack) HasHash(key ihash.Hash) (bool, error) {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	for _, ip := range pp.packs {
		entry, err := ip.idx.GetRaw(key)
		if err != nil {
			return false, err
		}

		if entry == nil {
			continue
		}

		return true, nil
	}

	return false, nil
}

func (pp *PackPack) addPack(packHash string) error {
	idx, err := NewIndexFromFile(indexPath(packHash, pp.path))
	if err != nil {
		return err
	}

	pf, err := iio.OpenFile(packPath(packHash, pp.path), os.O_RDONLY, 0755)
	if err != nil {
		return err
	}

	pp.mu.Lock()
	defer pp.mu.Unlock()

	pp.packs[packHash] = &packAndIndex{
		idx: idx,
		pr:  NewReaderSnappy(NewReader(pf)),
	}

	return nil
}

func (pp *PackPack) reloadPacks() error {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	return filepath.WalkDir(pp.path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		dir, file := path.Split(p)
		ext := path.Ext(file)
		key := strings.TrimSuffix(file, ext)

		if ext == ".pack" {
			if _, exists := pp.packs[key]; exists {
				return nil
			}

			idx, err := NewIndexFromFile(path.Join(dir, fmt.Sprintf("%s.idx", key)))
			if err != nil {
				return err
			}

			pr, err := iio.OpenFile(p, os.O_RDONLY, 0755)
			if err != nil {
				return err
			}

			pp.packs[key] = &packAndIndex{
				idx: idx,
				pr:  NewReaderSnappy(NewReader(pr)),
			}
		}

		return nil
	})
}

func (pp *PackPack) NewPackProcessing(numObjects int) (*PackProcessing, error) {
	packProc := &PackProcessing{
		tempPackNames:     make(map[string]struct{}),
		tempPath:          pp.tempPath,
		packFolder:        pp.path,
		maxObjectsPerPack: numObjects,

		idx: NewIndexWriter(),
		pp:  pp,
	}
	return packProc, packProc.newPack()
}

type PackProcessing struct {
	tempPackNames     map[string]struct{}
	tempPath          string
	packFolder        string
	maxObjectsPerPack int

	processingPackPath string
	elementsPacked     int

	idx *IndexWriter
	w   *WriterSnappy
	pp  *PackPack
}

func (pp *PackProcessing) closePack() error {
	if err := pp.w.Close(); err != nil {
		return err
	}

	name := pp.w.Hash()

	if err := WriteIndex(pp.idx, indexProcessingPath(name, pp.tempPath)); err != nil {
		return err
	}

	if err := iio.Rename(pp.processingPackPath, packProcessingPath(name, pp.tempPath)); err != nil {
		return err
	}

	pp.tempPackNames[name] = struct{}{}
	pp.elementsPacked = 0

	return nil
}

func (pp *PackProcessing) newPack() error {
	if pp.w != nil {
		if err := pp.closePack(); err != nil {
			return err
		}
	}

	tid := rand.NewSource(time.Now().Unix()).Int63()
	filename := fmt.Sprintf("tx-%d.pack.writting", tid)
	pn := path.Join(pp.tempPath, filename)
	f, err := iio.OpenFile(pn, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	pp.processingPackPath = pn

	pp.idx = NewIndexWriter()
	pp.w = NewWriterSnappy(NewWriter(f))

	return pp.w.WriteHeader()
}

func (pp *PackProcessing) WriteBlock(key []byte, value []byte) error {
	if pp.elementsPacked >= pp.maxObjectsPerPack {
		if err := pp.newPack(); err != nil {
			return err
		}
	}

	pos, err := pp.w.WriteBlock(key, value)
	if err != nil {
		return err
	}

	pp.idx.Add(key, pos, int64(len(value)))

	pp.elementsPacked++

	return nil
}

func (pp *PackProcessing) Commit() error {
	if err := pp.closePack(); err != nil {
		return err
	}

	for name := range pp.tempPackNames {
		// write first the index, because packpack is checking for new .pack files, not indexes
		if err := iio.Rename(indexProcessingPath(name, pp.tempPath), indexPath(name, pp.packFolder)); err != nil {
			return err
		}

		// TODO add packfiles in folder buckets too
		// after writting the index, we can move the packfile safely
		if err := iio.Rename(packProcessingPath(name, pp.tempPath), packPath(name, pp.packFolder)); err != nil {
			return err
		}

		if err := pp.pp.addPack(name); err != nil {
			return err
		}
	}

	return nil
}

func packPath(name, packPath string) string {
	return path.Join(packPath, fmt.Sprintf("%s.pack", name))

}

func packProcessingPath(name, packPath string) string {
	return path.Join(packPath, fmt.Sprintf("%s.pack.writting", name))
}

func indexPath(name, packPath string) string {
	return path.Join(packPath, fmt.Sprintf("%s.idx", name))
}

func indexProcessingPath(name, packPath string) string {
	return path.Join(packPath, fmt.Sprintf("%s.idx.writting", name))
}
