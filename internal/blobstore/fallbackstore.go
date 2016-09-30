// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"io"

	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"
)

// fallbackStore is a store.

var _ store = (*fallbackStore)(nil)

// fallbackStore holds the Stores to use as the blobstore.
// The first store is the read/write store. The remaining stores are used as
// read only stores in the event that migration is ongoing to the
// primary store.
type fallbackStore struct {
	stores []*Store
	// paralle arrays because i'm sloppy
	storeTypes []string
}

// NewFallbackStore returns a new fallbackStore which is backed by
// the given ProviderConfig. A gridfs store will use
// the given mgo.Database.
func NewFallbackStore(bsps []ProviderConfig) *Store {
	return &Store{newFallbackStore(bsps)}
}

func newFallbackStore(bsps []ProviderConfig) *fallbackStore {
	s := &fallbackStore{
		stores:     make([]*Store, len(bsps)),
		storeTypes: make([]string, len(bsps)),
	}
	for i, bsp := range bsps {
		s.storeTypes[i] = bsp.Type
		switch bsp.Type {
		case "gridfs":
			s.stores[i] = NewGridFSFromProviderConfig(&bsp)
		case "s3":
			s.stores[i] = NewS3(&bsp)
		case "localfs":
			s.stores[i] = NewLocalFS(&bsp)
		default:
			panic("unknown BloblStorageProvider: " + bsp.Type + " only gridfs or s3 are implemented")
		}
		logger.Debugf("%s blob storage provider configured by FaillbackStore: %v\n", bsp.Type, bsp)
	}
	return s
}

func (s *fallbackStore) Put(r io.Reader, name string, size int64, hash string, proof *ContentChallengeResponse) (*ContentChallenge, error) {
	return s.stores[0].Put(r, name, size, hash, proof)
}

func (s *fallbackStore) PutUnchallenged(r io.Reader, name string, size int64, hash string) error {
	return s.stores[0].PutUnchallenged(r, name, size, hash)
}

func (s *fallbackStore) Open(name string) (ReadSeekCloser, int64, error) {
	for i := range s.stores {
		f, st, err := s.stores[i].Open(name)
		if err == nil {
			return f, st, err
		}
		logger.Debugf("fallbackStore open %s not found in %s err was: %v trying next", name, s.storeTypes[i], err)
	}
	return nil, 0, errgo.Newf("file not found %s", name)
}

func (s *fallbackStore) Remove(name string) error {
	return s.stores[0].Remove(name)
}
