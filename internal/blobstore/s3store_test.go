// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"runtime"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/awstesting/mock"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/lsegal/gucumber"
	gc "gopkg.in/check.v1"
)

type s3StoreSuite struct{}

var _ = gc.Suite(&s3StoreSuite{})

func (s *s3StoreSuite) TestPutOpen(c *gc.C) {
	store := NewS3("testbucket")
	content := "some data"
	chal, err := store.Put(strings.NewReader(content), "x", int64(len(content)), hashOf(content), nil)
	c.Assert(err, gc.IsNil)
	c.Assert(chal, gc.IsNil)

	rc, length, err := store.Open("x")
	c.Assert(err, gc.IsNil)
	defer rc.Close()
	c.Assert(length, gc.Equals, int64(len(content)))

	data, err := ioutil.ReadAll(rc)
	c.Assert(err, gc.IsNil)
	c.Assert(string(data), gc.Equals, content)

	// Putting the resource again should generate a challenge.
	chal, err = store.Put(strings.NewReader(content), "y", int64(len(content)), hashOf(content), nil)
	c.Assert(err, gc.IsNil)
	c.Assert(chal, gc.NotNil)

	resp, err := NewContentChallengeResponse(chal, strings.NewReader(content))
	c.Assert(err, gc.IsNil)

	chal, err = store.Put(strings.NewReader(content), "y", int64(len(content)), hashOf(content), resp)
	c.Assert(err, gc.IsNil)
	c.Assert(chal, gc.IsNil)
}

func (s *s3StoreSuite) TestPutMock(c *gc.C) {
	svc := s3.New(mock.Session)
	memStatStart := &runtime.MemStats{}
	runtime.ReadMemStats(memStatStart)
	gucumber.World["start"] = memStatStart

	svc.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String("bucketmesilly"),
		Key:    aws.String("testKey"),
		Body:   bytes.NewReader([]byte("Hello World")),
	})
}

func hashOf(s string) string {
	h := NewHash()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}
