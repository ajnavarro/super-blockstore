package packfile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/klauspost/compress/s2"
)

type Reader struct {
	// TODO buffered reader. maybe separate the seeker from the sequencial reader?
	rc        io.ReadSeeker
	c         io.Closer
	gotHeader bool
}

func NewReader(rc io.ReadSeekCloser) (*Reader, error) {
	s2rs, err := s2.NewReader(rc).ReadSeeker(true, nil)
	if err != nil {
		return nil, err
	}

	return &Reader{
		rc: s2rs,
		c:  rc,
	}, nil
}

func (pr *Reader) Next() ([]byte, []byte, error) {
	if !pr.gotHeader {
		if err := pr.readHeader(); err != nil {
			return nil, nil, err
		}
	}

	bh, err := pr.readBlockHeader()
	if err != nil {
		return nil, nil, err
	}

	v := make([]byte, bh.Blocksize)
	_, err = io.ReadFull(pr.rc, v)
	if err != nil {
		return nil, nil, err
	}

	return bh.Key, v, err
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

func (pr *Reader) ReadValueAt(off int64) ([]byte, []byte, error) {
	_, err := pr.rc.Seek(off, io.SeekStart)
	if err != nil {
		return nil, nil, err
	}

	bh, err := pr.readBlockHeader()
	if err != nil {
		return nil, nil, err
	}

	v := make([]byte, bh.Blocksize)
	_, err = io.ReadFull(pr.rc, v)
	if err != nil {
		return nil, nil, err
	}

	return bh.Key, v, err
}

func (pr *Reader) Close() error {
	return pr.c.Close()
}

type BlockHeader struct {
	Key       []byte
	Blocksize uint32
}

func (pr *Reader) readBlockHeader() (*BlockHeader, error) {
	//block_header:
	//	key:[32]bytes (sha256)
	//	blocksize:uint64
	key := make([]byte, 32)

	if _, err := io.ReadFull(pr.rc, key); err != nil {
		return nil, err
	}

	var blocksize uint32
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
