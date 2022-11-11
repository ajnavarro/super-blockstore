package superblock

type DatastoreConfig struct {
	Folder string

	BlockCacheNumElements int
	PackMaxNumElements    int
}

func (cfg *DatastoreConfig) FillDefaults() {
	if cfg.BlockCacheNumElements == 0 {
		cfg.BlockCacheNumElements = 1000
	}

	if cfg.PackMaxNumElements == 0 {
		cfg.PackMaxNumElements = 1e6
	}
}
