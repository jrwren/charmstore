// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package elasticsearch

import (
	"testing"

	jujutesting "github.com/juju/testing"
	gc "gopkg.in/check.v1"
)

func TestPackage(t *testing.T) {
	ElasticSearchTestPackage(t, nil)
}

type IsolatedElasticSearchSuite struct {
	jujutesting.IsolationSuite
	ElasticSearchSuite
}

func (s *IsolatedElasticSearchSuite) SetUpSuite(c *gc.C) {
	s.IsolationSuite.SetUpSuite(c)
	s.ElasticSearchSuite.SetUpSuite(c)
}
func (s *IsolatedElasticSearchSuite) TearDownSuite(c *gc.C) {
	s.ElasticSearchSuite.TearDownSuite(c)
	s.IsolationSuite.TearDownSuite(c)
}
func (s *IsolatedElasticSearchSuite) SetUpTest(c *gc.C) {
	s.IsolationSuite.SetUpTest(c)
	s.ElasticSearchSuite.SetUpTest(c)
}
func (s *IsolatedElasticSearchSuite) TearDownTest(c *gc.C) {
	s.ElasticSearchSuite.TearDownTest(c)
	s.IsolationSuite.TearDownTest(c)
}

var _ = gc.Suite(&IsolatedElasticSearchSuite{})

func (s *IsolatedElasticSearchSuite) TestSuccessfulPostDocument(c *gc.C) {
	doc := map[string]string{
		"a": "b",
	}
	id, err := s.ES.PostDocument("testindex", "testtype", doc)
	c.Assert(err, gc.IsNil)
	c.Assert(id, gc.NotNil)
	var result map[string]string
	err = s.ES.GetDocument("testindex", "testtype", id, &result)
	c.Assert(err, gc.IsNil)
}

func (s *IsolatedElasticSearchSuite) TestSuccessfulPutNewDocument(c *gc.C) {
	doc := map[string]string{
		"a": "b",
	}
	err := s.ES.PutDocument("testindex", "testtype", "a", doc)
	c.Assert(err, gc.IsNil)
	var result map[string]string
	err = s.ES.GetDocument("testindex", "testtype", "a", &result)
	c.Assert(result["a"], gc.Equals, "b")
}

func (s *IsolatedElasticSearchSuite) TestSuccessfulPutUpdatedDocument(c *gc.C) {
	doc := map[string]string{
		"a": "b",
	}
	err := s.ES.PutDocument("testindex", "testtype", "a", doc)
	doc["a"] = "c"
	err = s.ES.PutDocument("testindex", "testtype", "a", doc)
	c.Assert(err, gc.IsNil)
	var result map[string]string
	err = s.ES.GetDocument("testindex", "testtype", "a", &result)
	c.Assert(result["a"], gc.Equals, "c")
}

func (s *IsolatedElasticSearchSuite) TestDelete(c *gc.C) {
	doc := map[string]string{
		"a": "b",
	}
	s.ES.PostDocument("testindex", "testtype", doc)
	err := s.ES.DeleteIndex("testindex")
	c.Assert(err, gc.IsNil)
}

func (s *IsolatedElasticSearchSuite) TestDeleteErrorOnNonExistingIndex(c *gc.C) {
	err := s.ES.DeleteIndex("nope")
	terr := err.(*ErrNotFound)
	c.Assert(terr.Message(), gc.Equals, "index nope not found")
}

func (s *IsolatedElasticSearchSuite) TestIndexesEmpty(c *gc.C) {
	indexes, err := s.ES.ListAllIndexes()
	c.Assert(err, gc.IsNil)
	c.Assert(indexes, gc.HasLen, 0)
}

func (s *IsolatedElasticSearchSuite) TestIndexesCreatedAutomatically(c *gc.C) {
	doc := map[string]string{"a": "b"}
	s.ES.PostDocument("testindex", "testtype", doc)
	indexes, err := s.ES.ListAllIndexes()
	c.Assert(err, gc.IsNil)
	c.Assert(indexes[0], gc.Equals, "testindex")
}
