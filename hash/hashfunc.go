package ihash

import "crypto/sha256"

const KeySize = 32

type Hash [KeySize]byte

func SumBytes(data []byte) Hash {
	return sha256.Sum256(data)
}
