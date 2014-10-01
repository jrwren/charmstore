// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/mgo.v2"

	"github.com/juju/charmstore/config"
	"github.com/juju/charmstore/internal/charmstore"
	"github.com/juju/charmstore/internal/elasticsearch"
)

func main() {
	err := populate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func populate() error {
	var confPath string
	if len(os.Args) == 2 {
		if _, err := os.Stat(os.Args[1]); err == nil {
			confPath = os.Args[1]
		}
	}
	if confPath == "" {
		return fmt.Errorf("usage: %s <config path>", filepath.Base(os.Args[0]))
	}
	conf, err := config.Read(confPath)
	if err != nil {
		return err
	}
	if conf.ESAddr == "" {
		return fmt.Errorf("No elasticsearch-addr specified in %s", confPath)
	}
	session, err := mgo.Dial(conf.MongoURL)
	if err != nil {
		return err
	}
	defer session.Close()
	db := session.DB("juju")
	store, err := charmstore.NewStore(db)
	if err != nil {
		return err
	}
	store.SetES(&elasticsearch.Database{conf.ESAddr})
	return store.ExportToElasticSearch()
}