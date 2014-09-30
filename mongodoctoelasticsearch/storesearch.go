// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package mongodoctoelasticsearch

import (
	"net/url"

	"github.com/juju/charmstore/internal/charmstore"
	"github.com/juju/charmstore/internal/elasticsearch"
	"github.com/juju/charmstore/internal/mongodoc"
	"github.com/juju/loggo"
)

var (
	logger = loggo.GetLogger("juju.charmstore.mongodoctoelasticsearch")
)

func Export(store *charmstore.Store, es *elasticsearch.Database) error {
	var result mongodoc.Entity
	iter := store.DB.Entities().Find(nil).Iter()
	for iter.Next(&result) {
		es.PutDocument("charmstore", "entity", url.QueryEscape(result.URL.String()), result)
	}
	if err := iter.Close(); err != nil {
		return err
	}
	return nil
}
