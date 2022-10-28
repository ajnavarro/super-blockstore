package packfile

type PackPack struct {
	path string
}

func NewPackPack(path string) *PackPack {
	return &PackPack{path: path}
}

func (pp *PackPack) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (pp *PackPack) Has(key []byte) (bool, error) {
	return false, nil
}
