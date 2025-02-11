package cache

import (
	"encoding/hex"
	"fmt"
	"hash"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

type blobWriter struct {
	storagedriver.FileWriter
	cacheHash string
	h         hash.Hash
}

func (bw *blobWriter) Commit() error {
	hash := hex.EncodeToString(bw.h.Sum(nil)[:])
	if bw.cacheHash != hash {
		return fmt.Errorf("expected %s hash, got %s", bw.cacheHash, hash)
	}
	return bw.FileWriter.Commit()
}

func (bw *blobWriter) Write(p []byte) (int, error) {
	bw.h.Write(p)
	return bw.FileWriter.Write(p)
}
