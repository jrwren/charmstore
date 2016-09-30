// Copyright 2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package blobstore // import "gopkg.in/juju/charmstore.v5-unstable/internal/blobstore"

// ProviderConfig holds configuration for where blobs are stored.
type ProviderConfig struct {
	// Type of the provider.  Currently gridfs or s3 are supported.
	Type string `json:"type"`

	// MongoAddr is the address of the mongodb database holding the gridfs.
	MongoAddr string `yaml:"mongo_addr,omitempty"`

	// MongoDBName is the name of the mongodb database holding the gridfs.
	MongoDBName string `yaml:"mongo_dbname,omitempty"`

	// BucketName to use with S3 or the GridFS Prefix to use with gridfs.
	BucketName string `yaml:"bucket_name,omitempty"`

	// Endpoint for using S3 api with non-S3 store such as swift or Raik CS.
	Endpoint string `json:"endpoint,omitempty"`

	// Region to use with S3.
	Region string `json:"region,omitempty"`

	// S3ForcePathStyle to use with S3.
	S3ForcePathStyle bool `json:"s3_force_path_style,omitempty"`

	// DisableSSL to use with S3.
	DisableSSL bool `json:"disable_ssl,omitempty"`

	// Key to use with S3.
	Key string `json:"key,omitempty"`

	// Secret to use with S3.
	Secret string `json:"secret,omitempty"`
}
