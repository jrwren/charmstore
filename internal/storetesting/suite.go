// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package storetesting

import (
	"github.com/juju/charmstore/internal/elasticsearch"
	jujutesting "github.com/juju/testing"
	gc "gopkg.in/check.v1"
)

type IsolatedMgoSuite struct {
	jujutesting.IsolationSuite
	jujutesting.MgoSuite
	elasticsearch.ElasticSearchSuite
}

func (s *IsolatedMgoSuite) SetUpSuite(c *gc.C) {
	s.IsolationSuite.SetUpSuite(c)
	s.MgoSuite.SetUpSuite(c)
	s.ElasticSearchSuite.SetUpSuite(c)
}

func (s *IsolatedMgoSuite) TearDownSuite(c *gc.C) {
	s.ElasticSearchSuite.TearDownSuite(c)
	s.MgoSuite.TearDownSuite(c)
	s.IsolationSuite.TearDownSuite(c)
}

func (s *IsolatedMgoSuite) SetUpTest(c *gc.C) {
	s.IsolationSuite.SetUpTest(c)
	s.MgoSuite.SetUpTest(c)
	s.ElasticSearchSuite.SetUpTest(c)
}

func (s *IsolatedMgoSuite) TearDownTest(c *gc.C) {
	s.ElasticSearchSuite.TearDownTest(c)
	s.MgoSuite.TearDownTest(c)
	s.IsolationSuite.TearDownTest(c)
}
