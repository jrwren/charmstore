// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/awstesting/mock"
	"github.com/aws/aws-sdk-go/service/s3"
	gc "gopkg.in/check.v1"
)

type s3StoreSuite struct{}

var _ = gc.Suite(&s3StoreSuite{})

func (s *s3StoreSuite) TestPutOpen(c *gc.C) {
	store := newS3(&ProviderConfig{BucketName: "charmstoretestbucket"})
	store.getS3 = testGetS3
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

func hashOf(s string) string {
	h := NewHash()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

var mockSession = newMockSession()

func newMockSession() client.ConfigProvider {
	x := mock.Session
	return x
}

// testGetS3 returns a fake s3.S3.
func testGetS3() *s3.S3 {
	s3 := s3.New(mockSession)
	s3.Handlers.Validate.Clear()
	s3.Handlers.Unmarshal.Clear()
	s3.Handlers.UnmarshalMeta.Clear()
	s3.Handlers.UnmarshalError.Clear()
	s3.Handlers.Send.Clear()
	s3.Handlers.Send.PushBackNamed(sendHandler)
	return s3
}

var fakebucket = map[string][]byte{}

var sendHandler = request.NamedHandler{Name: "fake.SendHandler", Fn: func(r *request.Request) {
	r.HTTPResponse = &http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte{})),
	}
	switch r.Operation.HTTPMethod {
	case "PUT":
		data, err := ioutil.ReadAll(r.HTTPRequest.Body)
		if err != nil {
			panic("could not read http request body")
		}
		fakebucket[r.Operation.HTTPPath] = data
	case "GET":
		data, ok := fakebucket[r.Operation.HTTPPath]
		if !ok {
			r.HTTPResponse.StatusCode = 404
		} else {
			goo := r.Data.(*s3.GetObjectOutput)
			goo.Body = ioutil.NopCloser(bytes.NewReader(data))
			l := int64(len(data))
			goo.ContentLength = &l
		}
	}
}}
