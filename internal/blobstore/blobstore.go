// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"crypto/sha512"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/juju/errors"
	"github.com/juju/loggo"
	"gopkg.in/errgo.v1"
	"gopkg.in/mgo.v2"

	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
)

var logger = loggo.GetLogger("charmstore.internal.blobstore")

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// NewHash is used to calculate checksums for the blob store.
func NewHash() hash.Hash {
	return sha512.New384()
}

// Store stores data blobs in mongodb, de-duplicating by
// blob hash.
type Store struct {
	uploadc   *mgo.Collection
	bsc       storage.BlobStorageClient
	container string

	// The following fields are given default values by
	// New but may be changed away from the defaults
	// if desired.

	// MinPartSize holds the minimum size of a multipart upload part.
	MinPartSize int64

	// MaxPartSize holds the maximum size of a multipart upload part.
	MaxPartSize int64

	// MaxParts holds the maximum number of parts that there
	// can be in a multipart upload.
	MaxParts int
}

// New returns a new blob store that writes to the given database,
// prefixing its collections with the given prefix, container for its Azure
// container and the given BlobStorageClient for Azure operations.
func New(db *mgo.Database, prefix, container string, bsc storage.BlobStorageClient) *Store {
	return &Store{
		uploadc:     db.C(prefix + ".upload"),
		bsc:         bsc,
		container:   container,
		MinPartSize: defaultMinPartSize,
		MaxParts:    defaultMaxParts,
		MaxPartSize: defaultMaxPartSize,
	}
}

// Put streams the content from the given reader into blob
// storage, with the provided name. The content should have the given
// size and hash.
func (s *Store) Put(r io.Reader, name string, size int64, hash string) error {
	// Check hash because juju/blobstore was hash checking.
	f, err := ioutil.TempFile("", "charmstore-blobstore")
	if err != nil {
		return errgo.Mask(err)
	}
	defer f.Close()
	defer os.Remove(f.Name())
	r2 := io.TeeReader(r, f)
	h := NewHash()
	io.Copy(h, r2)
	if hash != fmt.Sprintf("%x", h.Sum(nil)) {
		return errgo.New("hash mismatch")
	}

	f.Seek(0, 0)
	return s.bsc.CreateBlockBlobFromReader(s.container, name, uint64(size), f, nil)
}

// Open opens the entry with the given name. It returns an error
// with an ErrNotFound cause if the entry does not exist.
func (s *Store) Open(name string, index *mongodoc.MultipartIndex) (ReadSeekCloser, int64, error) {
	if index != nil {
		return newMultiReader(s, name, index)
	}
	r, bp, err := s.bsc.GetBlobAndProperties(s.container, name)
	if err != nil {
		if err2, ok := err.(storage.AzureStorageServiceError); ok && err2.StatusCode == 404 {
			return nil, 0, errgo.WithCausef(err, ErrNotFound, "")
		}
		if errors.IsNotFound(err) {
			return nil, 0, errgo.WithCausef(err, ErrNotFound, "")
		}
		return nil, 0, errgo.Mask(err)
	}
	r2, err := newReadSeekCloser(r)
	if err != nil {
		return nil, 0, err
	}
	return r2, bp.ContentLength, nil
}

// Remove the given name from the Store.
func (s *Store) Remove(name string, index *mongodoc.MultipartIndex) error {
	err := s.bsc.DeleteBlob(s.container, name, nil)
	if err2, ok := err.(storage.AzureStorageServiceError); ok && err2.StatusCode == 404 {
		// NOTE: tests are testing underlying error messages so: fake juju/blobstore (or underlying) response
		return errgo.WithCausef(errgo.Newf("resource at path \"global/"+name+"\" not found"), ErrNotFound, "")
	}
	if errors.IsNotFound(err) {
		return errgo.WithCausef(err, ErrNotFound, "")
	}
	return errgo.Mask(err)
}

// Take an io.ReadCloser, buffer it to temp, and make it a ReadSeekCloser.
type readSeekCloser struct {
	io.Reader
	c     io.Closer
	f     *os.File
	wrote bool
	eof   bool
}

func newReadSeekCloser(r io.ReadCloser) (*readSeekCloser, error) {
	f, err := ioutil.TempFile("", "charmstore-blobstore")
	if err != nil {
		return nil, err
	}
	s := &readSeekCloser{
		Reader: io.TeeReader(r, f),
		f:      f,
		c:      r,
	}
	return s, nil
}

func (s *readSeekCloser) Read(p []byte) (n int, err error) {
	if s.wrote {
		return s.f.Read(p)
	}
	n, err = s.Reader.Read(p)
	if err == io.EOF {
		s.wrote = true
		// The semantics of Reader with respect to EOF are variable.
		// https://golang.org/pkg/io/#Reader
		// multiReader assumes only of two possibilities.
		// If this is the first time EOF was read, do not return it.
		if !s.eof && n > 0 {
			s.eof = true
			err = nil
		}
	}
	return n, err
}

func (s *readSeekCloser) Seek(offset int64, whence int) (int64, error) {
	if !s.wrote {
		ioutil.ReadAll(s.Reader)
		s.wrote = true
	}
	s.eof = false
	return s.f.Seek(offset, whence)
}

func (s *readSeekCloser) Close() error {
	err1 := s.c.Close()
	err2 := s.f.Close()
	err3 := os.Remove(s.f.Name())
	if err1 != nil || err2 != nil || err3 != nil {
		return errgo.Notef(err1, "error closing stream and possible other errors: %v, %v", err2, err3)
	}
	return nil
}
