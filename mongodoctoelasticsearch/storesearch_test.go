// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package mongodoctoelasticsearch

import (
	"net/url"
	"testing"

	"github.com/juju/charmstore/internal/charmstore"
	"github.com/juju/charmstore/internal/elasticsearch"
	"github.com/juju/charmstore/internal/mongodoc"
	"github.com/juju/charmstore/internal/storetesting"
	jujutesting "github.com/juju/testing"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v4"
	charmtesting "gopkg.in/juju/charm.v4/testing"
)

func TestPackage(t *testing.T) {
	elasticsearch.ElasticSearchTestPackage(t, func(t2 *testing.T) {
		jujutesting.MgoTestPackage(t2, nil)
	})
}

type MongoDocToElasticSearchSuite struct {
	storetesting.IsolatedMgoSuite
}

var _ = gc.Suite(&MongoDocToElasticSearchSuite{})

var charms = map[string]string{"wordpress": "cs:precies/wordpress-23", "mysql": "cs:precise/mysql-42"}

func (s *MongoDocToElasticSearchSuite) TestSuccessfulExport(c *gc.C) {
	store, err := charmstore.NewStore(s.Session.DB("mongodoctoelasticsearch"))
	c.Assert(err, gc.IsNil)
	s.addCharmsToStore(store)
	err = Export(store, s.ES)
	c.Assert(err, gc.IsNil)

	var expected mongodoc.Entity
	var actual mongodoc.Entity
	for _, ref := range charms {
		store.DB.Entities().FindId(ref).One(&expected)
		err = s.ES.GetDocument("charmstore", "entity", url.QueryEscape(ref), &actual)
		c.Assert(err, gc.IsNil)
		// can't use deepequals because the pointers differ
		//c.Assert(expected, gc.DeepEquals, actual)

		// can't compare json strings because map orders come back different
		//expectedjson,_ = json.Marshal(expected)
		//actauljson,_ = json.Marshal(actual)
		//c.Assert(expectedjson, gc.Equals, actualjson)

		// wanted: a more complete way to compare
		c.Assert(expected.URL.String(), gc.Equals, actual.URL.String())
		c.Assert(expected.BlobHash256, gc.Equals, actual.BlobHash256)
		c.Assert(expected.Size, gc.Equals, actual.Size)
		c.Assert(expected.BlobName, gc.Equals, actual.BlobName)
		c.Assert(expected.CharmProvidedInterfaces, gc.DeepEquals, actual.CharmProvidedInterfaces)
		c.Assert(expected.CharmRequiredInterfaces, gc.DeepEquals, actual.CharmRequiredInterfaces)
		c.Assert(expected.BundleReadMe, gc.Equals, actual.BundleReadMe)
	}
}

func (s *MongoDocToElasticSearchSuite) addCharmsToStore(store *charmstore.Store) {
	for name, ref := range charms {
		charmArchive := charmtesting.Charms.CharmDir(name)
		url, _ := charm.ParseReference(ref)
		store.AddCharmWithArchive(url, charmArchive)
	}
}
