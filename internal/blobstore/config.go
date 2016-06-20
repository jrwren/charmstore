// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

// ProviderConfig holds configuration for where blobs are stored.
type ProviderConfig struct {
	// Name of the provider, currently gridfs or s3
	Name string

	// BucketName to use with S3
	BucketName string
}
