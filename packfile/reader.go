package packfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"

	"github.com/klauspost/compress/s2"
)

type reader interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}

type Reader struct {
	// TODO buffered reader. maybe separate the seeker from the sequencial reader?
	rc        reader
	gotHeader bool
}

func NewReader(rc reader) *Reader {
	return &Reader{
		rc: rc,
	}
}

func (pr *Reader) Next() ([]byte, *io.SectionReader, error) {
	if !pr.gotHeader {
		if err := pr.readHeader(); err != nil {
			return nil, nil, err
		}
	}

	bh, err := pr.readBlockHeader()
	if err != nil {
		return nil, nil, err
	}

	pos, err := pr.rc.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, nil, err
	}

	sr := io.NewSectionReader(pr.rc, pos, int64(bh.Blocksize))
	_, err = pr.rc.Seek(int64(bh.Blocksize), io.SeekCurrent)
	if err != nil {
		return nil, nil, err
	}

	return bh.Key, sr, err
}

func (pr *Reader) Skip() error {
	if !pr.gotHeader {
		if err := pr.readHeader(); err != nil {
			return err
		}
	}

	bh, err := pr.readBlockHeader()
	if err != nil {
		return err
	}

	_, err = pr.rc.Seek(int64(bh.Blocksize), io.SeekCurrent)
	if err != nil {
		return err
	}

	return nil
}

func (pr *Reader) ReadValueAt(off int64) ([]byte, *io.SectionReader, error) {
	_, err := pr.rc.Seek(off, io.SeekStart)
	if err != nil {
		return nil, nil, err
	}

	bh, err := pr.readBlockHeader()
	if err != nil {
		return nil, nil, err
	}

	pos, err := pr.rc.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, nil, err
	}

	sr := io.NewSectionReader(pr.rc, pos, int64(bh.Blocksize))

	return bh.Key, sr, err
}

type BlockHeader struct {
	Key       []byte
	Blocksize uint64
}

func (pr *Reader) readBlockHeader() (*BlockHeader, error) {
	//block_header:
	//	key:[32]bytes (sha256)
	//	blocksize:uint64
	key := make([]byte, 32)

	if _, err := io.ReadFull(pr.rc, key); err != nil {
		return nil, err
	}

	var blocksize uint64
	if err := binary.Read(pr.rc, binary.BigEndian, &blocksize); err != nil {
		return nil, err
	}

	// TODO check CRC

	return &BlockHeader{
		Key:       key,
		Blocksize: blocksize,
	}, nil
}

func (pr *Reader) readHeader() error {
	// header:
	//   "SPB" magic key:3 bytes
	//   version:uint32

	h := make([]byte, 3)
	if _, err := io.ReadFull(pr.rc, h); err != nil {
		return err
	}

	if !bytes.Equal(h, packSig) {
		return errors.New("signature doesn't match")
	}

	var version uint32
	if err := binary.Read(pr.rc, binary.BigEndian, &version); err != nil {
		return err
	}

	if version != packVersion {
		return errors.New("version not supported")
	}

	pr.gotHeader = true

	return nil
}

type ReaderSnappy struct {
	*Reader
	zr *s2.Reader
}

func NewReaderSnappy(pr *Reader) *ReaderSnappy {
	return &ReaderSnappy{
		Reader: pr,
		zr:     s2.NewReader(nil),
	}
}

func (pr *ReaderSnappy) Next() ([]byte, []byte, error) {
	k, sr, err := pr.Reader.Next()
	if err != nil {
		return nil, nil, err
	}

	pr.zr.Reset(sr)

	val, err := ioutil.ReadAll(pr.zr)
	if err != nil {
		return nil, nil, err
	}

	return k, val, nil
}

func (pr *ReaderSnappy) ReadValueAt(off int64) ([]byte, []byte, error) {
	k, sr, err := pr.Reader.ReadValueAt(off)
	if err != nil {
		return nil, nil, err
	}

	pr.zr.Reset(sr)

	val, err := ioutil.ReadAll(pr.zr)
	if err != nil {
		return nil, nil, err
	}

	return k, val, nil
}
