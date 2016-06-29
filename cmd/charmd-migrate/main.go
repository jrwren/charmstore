// Copyright 2012-2016 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package main // import "gopkg.in/juju/charmstore.v5-unstable/cmd/charmd-migrate"

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/juju/loggo"
	"gopkg.in/errgo.v1"
	"gopkg.in/macaroon-bakery.v1/bakery"
	"gopkg.in/macaroon-bakery.v1/httpbakery"
	"gopkg.in/mgo.v2"
	"gopkg.in/natefinch/lumberjack.v2"

	"gopkg.in/juju/charmstore.v5-unstable"
	"gopkg.in/juju/charmstore.v5-unstable/config"
	"gopkg.in/juju/charmstore.v5-unstable/elasticsearch"
	cs "gopkg.in/juju/charmstore.v5-unstable/internal/charmstore"
)

var (
	logger        = loggo.GetLogger("charmd-migrate")
	loggingConfig = flag.String("logging-config", "", "specify log levels for modules e.g. <root>=TRACE")
	migration     = flag.String("migrate", "", "specify a migration to run")
	list          = flag.Bool("list", false, "list available migrations")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [options] <config path>\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		os.Exit(2)
	}
	flag.Parse()

	if *list {
		for i, _ := range cs.Migrations {
			fmt.Println(i)
		}
		os.Exit(0)
	}

	if flag.NArg() != 1 {
		flag.Usage()
	}
	if *loggingConfig != "" {
		if err := loggo.ConfigureLoggers(*loggingConfig); err != nil {
			fmt.Fprintf(os.Stderr, "cannot configure loggers: %v", err)
			os.Exit(1)
		}
	}

	switch *migration {
	case "all":
		if err := serve(flag.Arg(0)); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
	case "":
		fmt.Fprintf(os.Stderr, "use -migrate to specify a migration\n")
	default:
		if err := runMigration(*migration); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
	}
}

func runMigration(migration string) error {
	migrate, ok := cs.Migrations[migration]
	if !ok {
		return errgo.Newf("could not find migration named %s\n", migration)
	}
	conf, err := config.Read(flag.Arg(0))
	if err != nil {
		return errgo.Mask(err)
	}
	db, err := getDB(conf)
	if err != nil {
		return errgo.Mask(err)
	}
	defer db.Session.Close()
	keyring := bakery.NewPublicKeyRing()
	err = addPublicKey(keyring, conf.IdentityLocation, conf.IdentityPublicKey)
	if err != nil {
		return errgo.Mask(err)
	}
	params := getParams(conf, keyring)
	err = cs.RunMigration(db, cs.ServerParams(params), migrate)
	if err != nil {
		return errgo.Mask(err)
	}
	return nil
}

func serve(confPath string) error {
	logger.Infof("reading configuration")
	conf, err := config.Read(confPath)
	if err != nil {
		return errgo.Notef(err, "cannot read config file %q", confPath)
	}

	db, err := getDB(conf)
	if err != nil {
		return err
	}
	defer db.Session.Close()
	var es *elasticsearch.Database
	if conf.ESAddr != "" {
		es = &elasticsearch.Database{
			Addr: conf.ESAddr,
		}
	}

	keyring := bakery.NewPublicKeyRing()
	err = addPublicKey(keyring, conf.IdentityLocation, conf.IdentityPublicKey)
	if err != nil {
		return errgo.Mask(err)
	}
	if conf.TermsLocation != "" {
		err = addPublicKey(keyring, conf.TermsLocation, conf.TermsPublicKey)
		if err != nil {
			return errgo.Mask(err)
		}
	}

	logger.Infof("setting up the API server")
	cfg := getParams(conf, keyring)

	_, err = charmstore.NewServer(db, es, "cs", cfg, charmstore.Legacy, charmstore.V4, charmstore.V5)
	if err != nil {
		return errgo.Notef(err, "cannot create new server at %q", conf.APIAddr)
	}
	return nil
}

func addPublicKey(ring *bakery.PublicKeyRing, loc string, key *bakery.PublicKey) error {
	if key != nil {
		return ring.AddPublicKeyForLocation(loc, false, key)
	}
	pubKey, err := httpbakery.PublicKeyForLocation(http.DefaultClient, loc)
	if err != nil {
		return errgo.Mask(err)
	}
	return ring.AddPublicKeyForLocation(loc, false, pubKey)
}

var mgoLogger = loggo.GetLogger("mgo")

func init() {
	mgo.SetLogger(mgoLog{})
}

type mgoLog struct{}

func (mgoLog) Output(calldepth int, s string) error {
	mgoLogger.LogCallf(calldepth+1, loggo.DEBUG, "%s", s)
	return nil
}

func getDB(conf *config.Config) (*mgo.Database, error) {
	logger.Infof("connecting to mongo")
	session, err := mgo.Dial(conf.MongoURL)
	if err != nil {
		return nil, errgo.Notef(err, "cannot dial mongo at %q", conf.MongoURL)
	}
	dbName := "juju"
	if conf.Database != "" {
		dbName = conf.Database
	}
	db := session.DB(dbName)
	return db, nil
}

func getParams(conf *config.Config, keyring *bakery.PublicKeyRing) charmstore.ServerParams {
	cfg := charmstore.ServerParams{
		AuthUsername:            conf.AuthUsername,
		AuthPassword:            conf.AuthPassword,
		IdentityLocation:        conf.IdentityLocation,
		IdentityAPIURL:          conf.IdentityAPIURL,
		TermsLocation:           conf.TermsLocation,
		AgentUsername:           conf.AgentUsername,
		AgentKey:                conf.AgentKey,
		StatsCacheMaxAge:        conf.StatsCacheMaxAge.Duration,
		MaxMgoSessions:          conf.MaxMgoSessions,
		HTTPRequestWaitDuration: conf.RequestTimeout.Duration,
		SearchCacheMaxAge:       conf.SearchCacheMaxAge.Duration,
		PublicKeyLocator:        keyring,
	}
	if conf.AuditLogFile != "" {
		cfg.AuditLogger = &lumberjack.Logger{
			Filename: conf.AuditLogFile,
			MaxSize:  conf.AuditLogMaxSize,
			MaxAge:   conf.AuditLogMaxAge,
		}
	}
	return cfg
}
