// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package charmstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"

import (
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/juju/testing/httptesting"
	gc "gopkg.in/check.v1"
	errgo "gopkg.in/errgo.v1"
	"gopkg.in/juju/charm.v6-unstable"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery"
	"gopkg.in/macaroon-bakery.v2-unstable/bakery/mgostorage"
	"gopkg.in/retry.v1"

	"gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"
	"gopkg.in/juju/charmstore.v5-unstable/internal/mongodoc"
	"gopkg.in/juju/charmstore.v5-unstable/internal/router"
	"gopkg.in/juju/charmstore.v5-unstable/internal/storetesting"
)

var serverParams = ServerParams{
	AuthUsername:        "test-user",
	AuthPassword:        "test-password",
	IdentityLocation:    "http://0.1.2.3",
	AzureStorageAccount: storage.StorageEmulatorAccountName,
}

type ServerSuite struct {
	storetesting.IsolatedMgoESSuite
}

var _ = gc.Suite(&ServerSuite{})

func (s *ServerSuite) TestNewServerWithNoVersions(c *gc.C) {
	h, err := NewServer(s.Session.DB("foo"), nil, serverParams, nil)
	c.Assert(err, gc.ErrorMatches, `charm store server must serve at least one version of the API`)
	c.Assert(h, gc.IsNil)
}

type versionResponse struct {
	Version string
	Path    string
}

func (s *ServerSuite) TestNewServerWithVersions(c *gc.C) {
	db := s.Session.DB("foo")
	serveVersion := func(vers string) NewAPIHandlerFunc {
		return func(p *Pool, config ServerParams, _ string) (HTTPCloseHandler, error) {
			return nopCloseHandler{
				router.HandleJSON(func(_ http.Header, req *http.Request) (interface{}, error) {
					return versionResponse{
						Version: vers,
						Path:    req.URL.Path,
					}, nil
				}),
			}, nil
		}
	}

	h, err := NewServer(db, nil, serverParams, map[string]NewAPIHandlerFunc{
		"version1": serveVersion("version1"),
	})
	c.Assert(err, gc.Equals, nil)
	defer h.Close()
	assertServesVersion(c, h, "version1")
	assertDoesNotServeVersion(c, h, "version2")
	assertDoesNotServeVersion(c, h, "version3")

	h, err = NewServer(db, nil, serverParams, map[string]NewAPIHandlerFunc{
		"version1": serveVersion("version1"),
		"version2": serveVersion("version2"),
	})
	c.Assert(err, gc.Equals, nil)
	defer h.Close()
	assertServesVersion(c, h, "version1")
	assertServesVersion(c, h, "version2")
	assertDoesNotServeVersion(c, h, "version3")

	h, err = NewServer(db, nil, serverParams, map[string]NewAPIHandlerFunc{
		"version1": serveVersion("version1"),
		"version2": serveVersion("version2"),
		"version3": serveVersion("version3"),
	})
	c.Assert(err, gc.Equals, nil)
	defer h.Close()
	assertServesVersion(c, h, "version1")
	assertServesVersion(c, h, "version2")
	assertServesVersion(c, h, "version3")

	h, err = NewServer(db, nil, serverParams, map[string]NewAPIHandlerFunc{
		"version1": serveVersion("version1"),
		"":         serveVersion(""),
	})
	c.Assert(err, gc.Equals, nil)
	defer h.Close()
	assertServesVersion(c, h, "")
	assertServesVersion(c, h, "version1")
}

func (s *ServerSuite) TestNewServerWithConfig(c *gc.C) {
	params := ServerParams{
		AuthUsername:     "test-user",
		AuthPassword:     "test-password",
		IdentityLocation: "http://0.1.2.3/",
	}
	serveConfig := func(p *Pool, config ServerParams, _ string) (HTTPCloseHandler, error) {
		return nopCloseHandler{
			router.HandleJSON(func(_ http.Header, req *http.Request) (interface{}, error) {
				return config, nil
			}),
		}, nil
	}
	h, err := NewServer(s.Session.DB("foo"), nil, params, map[string]NewAPIHandlerFunc{
		"version1": serveConfig,
	})
	c.Assert(err, gc.Equals, nil)
	defer h.Close()

	// The IdentityLocation field is filled out from the IdentityLocation
	// and the final slash is trimmed.

	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: h,
		URL:     "/version1/some/path",
		ExpectBody: ServerParams{
			AuthUsername:     "test-user",
			AuthPassword:     "test-password",
			IdentityLocation: "http://0.1.2.3",
			RootKeyPolicy: mgostorage.Policy{
				ExpiryDuration: defaultRootKeyExpiryDuration,
			},
		},
	})
}

func (s *ServerSuite) TestNewServerWithElasticSearch(c *gc.C) {
	params := ServerParams{
		AuthUsername:     "test-user",
		AuthPassword:     "test-password",
		IdentityLocation: "http://0.1.2.3",
	}
	serveConfig := func(p *Pool, config ServerParams, _ string) (HTTPCloseHandler, error) {
		return nopCloseHandler{
			router.HandleJSON(func(_ http.Header, req *http.Request) (interface{}, error) {
				return config, nil
			}),
		}, nil
	}
	h, err := NewServer(
		s.Session.DB("foo"),
		&SearchIndex{s.ES, s.TestIndex},
		params,
		map[string]NewAPIHandlerFunc{
			"version1": serveConfig,
		},
	)
	c.Assert(err, gc.Equals, nil)
	defer h.Close()
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: h,
		URL:     "/version1/some/path",
		ExpectBody: ServerParams{
			AuthUsername:     "test-user",
			AuthPassword:     "test-password",
			IdentityLocation: "http://0.1.2.3",
			RootKeyPolicy: mgostorage.Policy{
				ExpiryDuration: defaultRootKeyExpiryDuration,
			},
		},
	})
}

func (s *ServerSuite) TestServerStartsBlobstoreGC(c *gc.C) {
	store := s.newStore(c, "juju_test")
	defer store.Close()

	uploadIds := make(map[string]int)

	// Create an unowned upload that's immediately out of date.
	uploadId0 := putMultipart(c, store.BlobStore, time.Now(), "abcdefghijklmnopqrstuvwxy")
	uploadIds[uploadId0] = 0

	// Create an owned upload that's immediately out of date.
	uploadId1 := putMultipart(c, store.BlobStore, time.Now(), "abcdefghijklmnopqrstuvwxy")
	err := store.BlobStore.SetOwner(uploadId1, resourceUploadOwner(&mongodoc.Resource{
		BaseURL:  charm.MustParseURL("cs:precise/wordpress"),
		Name:     "something",
		Revision: 2,
	}), time.Now())
	c.Assert(err, gc.Equals, nil)
	uploadIds[uploadId1] = 1

	// We'd like to create an owned upload that's owned by
	// a resource, but that involves a bunch of duplicated
	// logic (we'd need to insert the resource doc manually)
	// so doesn't give us that much extra confidence.
	// Instead, we rely on StoreSuite.TestIsUploadOwnedBy
	// to check the connection between the resource owner
	// and isUploadOwnedBy.

	params := ServerParams{
		AuthUsername:     "test-user",
		AuthPassword:     "test-password",
		IdentityLocation: "http://0.1.2.3",
		RunBlobStoreGC:   true,
	}
	h, err := NewServer(s.Session.DB("juju_test"), nil, params, nopAPI)
	c.Assert(err, gc.Equals, nil)
	defer h.Close()

	// The blob should be garbage-collected immediately but because
	// it's running asynchronously, it may be delayed.
	attempt := retry.Regular{
		Total: 1 * time.Second,
		Delay: 50 * time.Millisecond,
	}
	for a := attempt.Start(nil); len(uploadIds) > 0 && a.Next(); {
		for id := range uploadIds {
			_, err := store.BlobStore.UploadInfo(id)
			if err != nil {
				c.Assert(errgo.Cause(err), gc.Equals, blobstore.ErrNotFound, gc.Commentf("err: %v", err))
				delete(uploadIds, id)
			}
		}
	}
	if len(uploadIds) > 0 {
		c.Fatalf("not all uploads removed; remaining: %v", uploadIds)
	}
}

func assertServesVersion(c *gc.C, h http.Handler, vers string) {
	path := vers
	if path != "" {
		path = "/" + path
	}
	httptesting.AssertJSONCall(c, httptesting.JSONCallParams{
		Handler: h,
		URL:     path + "/some/path",
		ExpectBody: versionResponse{
			Version: vers,
			Path:    "/some/path",
		},
	})
}

func assertDoesNotServeVersion(c *gc.C, h http.Handler, vers string) {
	rec := httptesting.DoRequest(c, httptesting.DoRequestParams{
		Handler: h,
		URL:     "/" + vers + "/some/path",
	})
	c.Assert(rec.Code, gc.Equals, http.StatusNotFound)
}

type nopCloseHandler struct {
	http.Handler
}

func (nopCloseHandler) Close() {
}

var nopAPI = map[string]NewAPIHandlerFunc{
	"unused": func(*Pool, ServerParams, string) (HTTPCloseHandler, error) {
		return nopCloseHandler{http.NewServeMux()}, nil
	},
}

func (s *ServerSuite) newStore(c *gc.C, dbName string) *Store {
	p, err := NewPool(s.Session.DB(dbName), nil, &bakery.NewServiceParams{}, ServerParams{
		MinUploadPartSize: 10,
	})
	c.Assert(err, gc.Equals, nil)
	store := p.Store()
	defer p.Close()
	return store
}
