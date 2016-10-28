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
	"github.com/aws/aws-sdk-go/aws/credentials"
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
	return &Store{newS3(pc)}
}

func newS3(pc *ProviderConfig) *s3Store {
	getter := getS3(pc)
	svc := getter()
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: &pc.BucketName,
	})
	if err != nil {
		log.Println("Failed to create bucket", err)
	}
	return &s3Store{
		bucket: pc.BucketName,
		getS3:  getter,
	}
}

func (s *s3Store) Put(r io.Reader, name string, size int64, hash string, proof *ContentChallengeResponse) (_ *ContentChallenge, err error) {
	err = s.PutUnchallenged(r, name, size, hash)
	return
}

func (s *s3Store) PutUnchallenged(r io.Reader, name string, size int64, hash string) error {
	svc := s.getS3()
	f, err := ioutil.TempFile("", "s3store")
	if err != nil {
		return errgo.Mask(err)
	}
	defer os.Remove(f.Name())
	io.Copy(f, r)
	f.Seek(0, 0)
	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
		Body:   f,
	})
	if err != nil {
		logger.Errorf("put failed :%s", err)
		return errgo.Mask(err)
	}
	logger.Debugf("successful put %s in bucket %s", name, s.bucket)
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

func (s *s3Store) StatAll() ([]BlobStoreStat, error) {
	return nil, nil
}

func getS3(pc *ProviderConfig) func() *s3.S3 {
	c := defaults.Get().Config.WithCredentialsChainVerboseErrors(true).WithRegion("us-east-1").
		WithCredentials(credentials.NewStaticCredentials(pc.Key, pc.Secret, ""))
	if pc.DisableSSL {
		c = c.WithDisableSSL(true)
	}
	if "" != pc.Endpoint {
		c = c.WithEndpoint(pc.Endpoint)
	}
	if "" != pc.Region {
		c = c.WithRegion(pc.Region)
	}
	if pc.S3ForcePathStyle {
		c = c.WithS3ForcePathStyle(true)
	}

	return func() *s3.S3 {
		sess := session.New(c)
		return s3.New(sess)
	}
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
