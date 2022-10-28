package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/ajnavarro/super-blockstorage/packfile"
	zim "github.com/akhenakh/gozim"
)

func main() {
	zr, err := zim.NewReader("/home/ajnavarro/Downloads/wikipedia_es_all_maxi_2022-04.zim", false)
	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile("wikipediasnappy.pack", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	if err != nil {
		panic(err)
	}

	pr := packfile.NewWriterSnappy(packfile.NewWriter(f))
	idx := packfile.NewIndex()

	err = pr.WriteHeader()
	if err != nil {
		panic(err)
	}

	count := 0

	for art := range zr.ListArticles() {
		if count == 1000000 {
			break
		}
		if art.Title == "" {
			continue
		}
		fmt.Println("writting article:", art.Title)
		key := []byte(art.Title)
		val, err := art.Data()
		if err != nil {
			panic(err)
		}
		fmt.Println("VAL", len(val))
		if len(val) == 0 {
			continue
		}

		pos, err := pr.WriteBlock(key, bytes.NewReader(val))
		if err != nil {
			panic(err)
		}

		idx.Add(key, pos)

		count++

		fmt.Println("Count", count)
	}

	err = pr.Close()
	if err != nil {
		panic(err)
	}

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
