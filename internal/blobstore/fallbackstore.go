// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"io"

	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"
)

// fallbackStore holds the Stores to use as the blobstore.
// The first store is the read/write store. The remaining stores are used as
// read only stores in the event that migration is ongoing to the
// primary store.
type fallbackStore struct {
	stores []Store
}

// NewFallbackStore returns a new fallbackStore which is backed by
// the given ProviderConfig. A gridfs store will use
// the given mgo.Database.
func NewFallbackStore(bsps []ProviderConfig, db *mgo.Database) *fallbackStore {
	s := &fallbackStore{
		stores: make([]Store, len(bsps)),
	}
	for i, bsp := range bsps {
		switch bsp.Name {
		case "gridfs":
			s.stores[i] = New(db, "entitystore")
		case "s3":
			s.stores[i] = NewS3(bsp.BucketName)
		default:
			panic("unknown BloblStorageProvider: " + bsp.Name + " only gridfs or s3 are implemented")
		}
	}
	return s
}

func (s *fallbackStore) Put(r io.ReadSeeker, name string, size int64, hash string, proof *ContentChallengeResponse) (*ContentChallenge, error) {
	return s.stores[0].Put(r, name, size, hash, proof)
}

func (s *fallbackStore) PutUnchallenged(r io.ReadSeeker, name string, size int64, hash string) error {
	return s.stores[0].PutUnchallenged(r, name, size, hash)
}

func (s *fallbackStore) Open(name string) (ReadSeekCloser, int64, error) {
	for i := range s.stores {
		f, s, err := s.stores[i].Open(name)
		if err == nil {
			return f, s, err
		}
	}
	return nil, 0, errgo.Newf("file not found %s", name)
}

func (s *fallbackStore) Remove(name string) error {
	return s.stores[0].Remove(name)
}
