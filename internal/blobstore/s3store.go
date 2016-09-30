// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gopkg.in/errgo.v1"
)

type s3Store struct {
	bucket string
	getS3  func() *s3.S3
}

// NewS3 createa a new S3 backed blobstore Store
func NewS3(pc *ProviderConfig) *Store {
	bucket := pc.BucketName
	return &Store{newS3(bucket)}
}

func newS3(bucket string) *s3Store {
	svc := getS3()
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		log.Println("Failed to create bucket", err)
	}
	return &s3Store{
		bucket: bucket,
		getS3:  getS3,
	}
}

func (s *s3Store) Put(r io.Reader, name string, size int64, hash string, proof *ContentChallengeResponse) (*ContentChallenge, error) {
	svc := s.getS3()
	reader, ok := r.(io.ReadSeeker)
	if !ok {
		panic("cannot cast to ReadSeeker")
	}
	_, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
		Body:   reader,
	})
	if err != nil {
		return nil, errgo.Mask(err)
	}
	return nil, nil
}

func (s *s3Store) PutUnchallenged(r io.Reader, name string, size int64, hash string) error {
	return nil
}

func (s *s3Store) Open(name string) (ReadSeekCloser, int64, error) {
	svc := s.getS3()
	req, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
	})
	if err != nil {
		return nil, 0, errgo.Mask(err)
	}
	if req.Body == nil {
		return nil, 0, errgo.Newf("body was empty")
	}
	data, err := ioutil.ReadAll(req.Body) // JRW: If only body were Seeker
	if err != nil {
		return nil, 0, errgo.Mask(err)
	}
	r := nopCloser(bytes.NewReader(data)) // JRW: *cringe*
	return r, *req.ContentLength, nil
}

func (s *s3Store) Remove(name string) error {
	return nil
}

func getS3() *s3.S3 {
	c := defaults.Get().Config.WithCredentialsChainVerboseErrors(true).WithRegion("us-east-1")
	if "" == os.Getenv("AWS_REGION") {
		c = c.WithRegion("us-east-1")
	}
	sess := session.New(c)
	return s3.New(sess)
}

type nopCloserReadSeeker struct {
	io.ReadSeeker
}

func (nopCloserReadSeeker) Close() error {
	return nil
}

// nopCloser returns a ReadSeekCloser with a no-op Close method
// wrapping the provided ReadSeeker r.
func nopCloser(r io.ReadSeeker) ReadSeekCloser {
	return nopCloserReadSeeker{r}
}
