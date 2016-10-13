// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"io/ioutil"
	"strings"

	"github.com/ncw/swift/swifttest"
	gc "gopkg.in/check.v1"
)

type swiftStoreSuite struct {
	srv *swifttest.SwiftServer
}

var _ = gc.Suite(&swiftStoreSuite{})

func (s *swiftStoreSuite) SetUpSuite(c *gc.C) {
	srv, err := swifttest.NewSwiftServer("localhost")
	if err != nil {
		c.Log(err)
		c.Fail()
	}
	s.srv = srv
}

func (s *swiftStoreSuite) TestPutOpen(c *gc.C) {
	store := newSwift(&ProviderConfig{
		BucketName: "charmstoretestbucket",
		Key:        "swifttest",
		Secret:     "swifttest",
		Endpoint:   s.srv.AuthURL,
	})
	//store.getSwiftConn = testGetSwift
	content := "some data"
	err := store.PutUnchallenged(strings.NewReader(content), "x", int64(len(content)), hashOf(content))
	c.Assert(err, gc.IsNil)

	rc, length, err := store.Open("x")
	c.Assert(err, gc.IsNil)
	defer rc.Close()
	c.Assert(length, gc.Equals, int64(len(content)))

	data, err := ioutil.ReadAll(rc)
	c.Assert(err, gc.IsNil)
	c.Assert(string(data), gc.Equals, content)

}
