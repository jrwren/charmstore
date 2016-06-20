// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

import (
	"bufio"
	"io"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gopkg.in/errgo.v1"
)

type s3Store struct {
	bucket string
}

// NewS3 createa a new S3 backed blobstore Store
func NewS3(bucket string) Store {
	return &s3Store{
		bucket: bucket,
	}
}

func (s *s3Store) Put(r io.Reader, name string, size int64, hash string, proof *ContentChallengeResponse) (*ContentChallenge, error) {
	svc := getS3()
	b := bufio.NewReader(r)
	_, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
		Body:   b,
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
	svc := getS3()
	req, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
	})
	if err != nil {
		return nil, 0, errgo.Mask(err)
	}
	r := ioutil.NopCloser(bufio.NewReader(req.Body)).(ReadSeekCloser)
	return r, *req.ContentLength, nil
}

func (s *s3Store) Remove(name string) error {
	return nil
}

func getS3() *s3.S3 {
	c := defaults.Get().Config.WithCredentialsChainVerboseErrors(true)
	sess := session.New(c)
	return s3.New(sess)
}
