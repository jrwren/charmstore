// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"io"
	"log"

	"github.com/ncw/swift"
	"gopkg.in/errgo.v1"
)

type swiftStore struct {
	bucket       string
	getSwiftConn func() *swift.Connection
}

// NewSwift createa a new swift backed blobstore Store
func NewSwift(pc *ProviderConfig) *Store {
	return &Store{newSwift(pc)}
}

func newSwift(pc *ProviderConfig) *swiftStore {
	getter := getswift(pc)
	svc := getter()
	err := svc.ContainerCreate(pc.BucketName, nil)
	if err != nil {
		log.Println("Failed to create bucket", err)
	}
	return &swiftStore{
		bucket:       pc.BucketName,
		getSwiftConn: getter,
	}
}

func (s *swiftStore) Put(r io.Reader, name string, size int64, hash string, proof *ContentChallengeResponse) (*ContentChallenge, error) {
	panic("why is this even part of the interface?")
}

func (s *swiftStore) PutUnchallenged(r io.Reader, name string, size int64, hash string) error {
	svc := s.getSwiftConn()
	_, err := svc.ObjectPut(s.bucket, name, r, true, "", "", nil)
	if err != nil {
		logger.Errorf("put failed :%s", err)
		return errgo.Mask(err)
	}
	logger.Debugf("successful put %s in bucket %s", name, s.bucket)
	return nil
}

func (s *swiftStore) Open(name string) (ReadSeekCloser, int64, error) {
	svc := s.getSwiftConn()
	oof, _, err := svc.ObjectOpen(s.bucket, name, true, nil)
	if err != nil {
		return nil, 0, errgo.Mask(err)
	}
	len, err := oof.Length()
	if err != nil {
		oof.Close()
		return nil, 0, errgo.Mask(err)
	}
	return oof, len, nil
}

func (s *swiftStore) Remove(name string) error {
	return s.getSwiftConn().ObjectDelete(s.bucket, name)
}

func getswift(pc *ProviderConfig) func() *swift.Connection {
	return func() *swift.Connection {
		return &swift.Connection{
			UserName: pc.Key,
			ApiKey:   pc.Secret,
			AuthUrl:  pc.Endpoint,
		}
	}
}
