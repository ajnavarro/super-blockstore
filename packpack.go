package superblock

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	ihash "github.com/ajnavarro/super-blockstorage/hash"
	"github.com/ajnavarro/super-blockstorage/iio"
	"github.com/ajnavarro/super-blockstorage/packfile"
)

// PackPack contains all the logic needed to get by key blocks from several packfiles.
// It will use indexes if available
type PackPack struct {
	path string

	mu    sync.RWMutex
	packs map[string]*packAndIndex
}

type packAndIndex struct {
	idx *packfile.Index
	pr  *packfile.Reader
}

func NewPackPack(path string) *PackPack {
	return &PackPack{
		path:  path,
		packs: make(map[string]*packAndIndex),
	}
}

func (pp *PackPack) GetHash(key ihash.Hash) ([]byte, error) {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	for _, ip := range pp.packs {
		entry, err := ip.idx.GetRaw(key)
		if err != nil {
			return nil, err
		}

		if entry == nil {
			continue
		}
		_, v, err := ip.pr.ReadValueAt(entry.Offset)
		if err != nil {
			return nil, err
		}

		return ioutil.ReadAll(v)
	}

	return nil, nil
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

func (pp *PackPack) AddPack(packHash string) error {
	idx, err := packfile.NewIndexFromFile(IndexPath(packHash, pp.path))
	if err != nil {
		return err
	}

	pf, err := iio.OpenFile(PackPath(packHash, pp.path), os.O_RDONLY, 0755)
	if err != nil {
		return err
	}

	pp.mu.Lock()
	defer pp.mu.Unlock()

	pp.packs[packHash] = &packAndIndex{
		idx: idx,
		pr:  packfile.NewReader(pf),
	}

	return nil
}

func (pp *PackPack) reloadPacks() error {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	return filepath.WalkDir(path.Join(pp.path, packFolder), func(p string, d fs.DirEntry, err error) error {
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

			idx, err := packfile.NewIndexFromFile(path.Join(dir, fmt.Sprintf("%s.idx", key)))
			if err != nil {
				return err
			}

			pr, err := iio.OpenFile(p, os.O_RDONLY, 0755)
			if err != nil {
				return err
			}

			pp.packs[key] = &packAndIndex{
				idx: idx,
				pr:  packfile.NewReader(pr),
			}
		}

		return nil
	})
}

func NewPackProcessing(p string) (*packfile.Writer, string, error) {
	tid := rand.NewSource(time.Now().Unix()).Int63()
	filename := fmt.Sprintf("tx-%d.pack.writting", tid)
	pn := path.Join(p, processingFolder, filename)
	f, err := iio.OpenFile(pn, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return nil, pn, err
	}

	pw := packfile.NewWriter(f)
	if err := pw.WriteHeader(); err != nil {
		return nil, pn, err
	}

	return pw, pn, nil
}

func PackPath(name, packPath string) string {
	return path.Join(packPath, packFolder, fmt.Sprintf("%s.pack", name))

}

func IndexPath(name, packPath string) string {
	return path.Join(packPath, packFolder, fmt.Sprintf("%s.idx", name))
}

func IndexProcessingPath(name, packPath string) string {
	return path.Join(packPath, processingFolder, fmt.Sprintf("%s.idx.writting", name))
}
