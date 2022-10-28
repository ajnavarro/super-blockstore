package main

import (
	"bytes"
	"io"
	"os"

	"github.com/ajnavarro/super-blockstorage/packfile"
)

func main() {
	f, err := os.Open("wikipediasnappy.pack")
	if err != nil {
		panic(err)
	}

	pr := packfile.NewReaderSnappy(packfile.NewReader(f))

	f2, err := os.OpenFile("wikipediagzip.pack", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	if err != nil {
		panic(err)
	}

	pw := packfile.NewWriterSnappy(packfile.NewWriter(f2))

	if err := pw.WriteHeader(); err != nil {
		panic(err)
	}

	idx := packfile.NewIndex()

	for {
		k, v, err := pr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		pos, err := pw.WriteBlock(k, bytes.NewBuffer(v))
		if err != nil {
			panic(err)
		}

		idx.Add(k, pos)
	}

	pw.Close()

	fidx, err := os.OpenFile("wikipediagzip.idx", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	if err != nil {
		panic(err)
	}
	defer fidx.Close()

	_, err = idx.WriteTo(fidx)
	if err != nil {
		panic(err)
	}
}
