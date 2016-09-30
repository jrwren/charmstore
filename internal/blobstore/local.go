// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"io"
	"os"
	"path"

	"github.com/juju/loggo"
)

var logger = loggo.GetLogger("charmstore.internal.blobstore")

type localFSStore struct {
	path string
}

func NewLocalFS(pc *ProviderConfig) *Store {
	return &Store{&localFSStore{path: pc.BucketName}}
}

func (s *localFSStore) Put(r io.Reader, name string, size int64, hash string, proof *ContentChallengeResponse) (*ContentChallenge, error) {
	w, err := os.Create(path.Join(s.path, name))
	if err != nil {
		return nil, err
	}
	io.Copy(w, r)
	return nil, nil
}

func (s *localFSStore) PutUnchallenged(r io.Reader, name string, size int64, hash string) error {
	return nil
}

func (s *localFSStore) Open(name string) (ReadSeekCloser, int64, error) {
	f, err := os.Open(path.Join(s.path, name))
	if err != nil {
		return nil, 0, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, stat.Size(), nil
}

func (s *localFSStore) Remove(name string) error {
	return os.Remove(name)
}
