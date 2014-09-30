// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package charmstore

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"sort"
	"time"

	"github.com/juju/errgo"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/juju/charm.v4"
	"gopkg.in/juju/charm.v4/testing"

	"github.com/juju/charmstore/internal/blobstore"
	"github.com/juju/charmstore/internal/mongodoc"
	"github.com/juju/charmstore/internal/storetesting"
	"github.com/juju/charmstore/params"
)

type StoreSuite struct {
	storetesting.IsolatedMgoSuite
}

var _ = gc.Suite(&StoreSuite{})

func (s *StoreSuite) checkAddCharm(c *gc.C, ch charm.Charm) {
	store, err := NewStore(s.Session.DB("foo"))
	c.Assert(err, gc.IsNil)
	url := mustParseReference("cs:precise/wordpress-23")

	// Add the charm to the store.
	beforeAdding := time.Now()
	err = store.AddCharmWithArchive(url, ch)
	c.Assert(err, gc.IsNil)
	afterAdding := time.Now()

	var doc mongodoc.Entity
	err = store.DB.Entities().FindId("cs:precise/wordpress-23").One(&doc)
	c.Assert(err, gc.IsNil)

	// The entity doc has been correctly added to the mongo collection.
	size, hash := mustGetSizeAndHash(ch)
	sort.Strings(doc.CharmProvidedInterfaces)
	sort.Strings(doc.CharmRequiredInterfaces)

	// Check the upload time and then reset it to its zero value
	// so that we can test the deterministic parts later.
	c.Assert(doc.UploadTime, jc.TimeBetween(beforeAdding, afterAdding))

	doc.UploadTime = time.Time{}

	blobName := doc.BlobName
	c.Assert(blobName, gc.Matches, "[0-9a-z]+")
	doc.BlobName = ""
	c.Assert(doc, jc.DeepEquals, mongodoc.Entity{
		URL:                     url,
		BaseURL:                 mustParseReference("cs:wordpress"),
		BlobHash:                hash,
		Size:                    size,
		CharmMeta:               ch.Meta(),
		CharmActions:            ch.Actions(),
		CharmConfig:             ch.Config(),
		CharmProvidedInterfaces: []string{"http", "logging", "monitoring"},
		CharmRequiredInterfaces: []string{"mysql", "varnish"},
	})

	// The charm archive has been properly added to the blob store.
	r, obtainedSize, err := store.BlobStore.Open(blobName)
	c.Assert(err, gc.IsNil)
	c.Assert(obtainedSize, gc.Equals, size)
	data, err := ioutil.ReadAll(r)
	c.Assert(err, gc.IsNil)
	charmArchive, err := charm.ReadCharmArchiveBytes(data)
	c.Assert(err, gc.IsNil)
	c.Assert(charmArchive.Meta(), jc.DeepEquals, ch.Meta())
	c.Assert(charmArchive.Config(), jc.DeepEquals, ch.Config())
	c.Assert(charmArchive.Actions(), jc.DeepEquals, ch.Actions())
	c.Assert(charmArchive.Revision(), jc.DeepEquals, ch.Revision())

	// Try inserting the charm again - it should fail because the charm is
	// already there.
	err = store.AddCharmWithArchive(url, ch)
	c.Assert(errgo.Cause(err), gc.Equals, params.ErrDuplicateUpload)
}

func (s *StoreSuite) checkAddBundle(c *gc.C, bundle charm.Bundle) {
	store, err := NewStore(s.Session.DB("foo"))
	c.Assert(err, gc.IsNil)
	url := mustParseReference("cs:bundle/wordpress-simple-42")

	// Add the bundle to the store.
	beforeAdding := time.Now()
	err = store.AddBundleWithArchive(url, bundle)
	c.Assert(err, gc.IsNil)
	afterAdding := time.Now()

	var doc mongodoc.Entity
	err = store.DB.Entities().FindId("cs:bundle/wordpress-simple-42").One(&doc)
	c.Assert(err, gc.IsNil)
	sort.Sort(orderedURLs(doc.BundleCharms))

	// Check the upload time and then reset it to its zero value
	// so that we can test the deterministic parts later.
	c.Assert(doc.UploadTime, jc.TimeBetween(beforeAdding, afterAdding))
	doc.UploadTime = time.Time{}

	// The blob name is random, but we check that it's
	// in the correct format, and non-empty.
	blobName := doc.BlobName
	c.Assert(blobName, gc.Matches, "[0-9a-z]+")
	doc.BlobName = ""

	// The entity doc has been correctly added to the mongo collection.
	size, hash := mustGetSizeAndHash(bundle)
	c.Assert(doc, jc.DeepEquals, mongodoc.Entity{
		URL:          url,
		BaseURL:      mustParseReference("cs:wordpress-simple"),
		BlobHash:     hash,
		Size:         size,
		BundleData:   bundle.Data(),
		BundleReadMe: bundle.ReadMe(),
		BundleCharms: []*charm.Reference{
			mustParseReference("mysql"),
			mustParseReference("wordpress"),
		},
		BundleMachineCount: newInt(2),
		BundleUnitCount:    newInt(2),
	})

	// The bundle archive has been properly added to the blob store.
	r, obtainedSize, err := store.BlobStore.Open(blobName)
	c.Assert(err, gc.IsNil)
	c.Assert(obtainedSize, gc.Equals, size)
	data, err := ioutil.ReadAll(r)
	c.Assert(err, gc.IsNil)
	bundleArchive, err := charm.ReadBundleArchiveBytes(data)
	c.Assert(err, gc.IsNil)
	c.Assert(bundleArchive.Data(), jc.DeepEquals, bundle.Data())
	c.Assert(bundleArchive.ReadMe(), jc.DeepEquals, bundle.ReadMe())

	// Try inserting the bundle again - it should fail because the bundle is
	// already there.
	err = store.AddBundleWithArchive(url, bundle)
	c.Assert(err, gc.Equals, params.ErrDuplicateUpload)
}

type orderedURLs []*charm.Reference

func (o orderedURLs) Less(i, j int) bool {
	return o[i].String() < o[j].String()
}

func (o orderedURLs) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}

func (o orderedURLs) Len() int {
	return len(o)
}

var urlFindingTests = []struct {
	inStore []string
	expand  string
	expect  []string
}{{
	inStore: []string{"cs:precise/wordpress-23"},
	expand:  "wordpress",
	expect:  []string{"cs:precise/wordpress-23"},
}, {
	inStore: []string{"cs:precise/wordpress-23", "cs:precise/wordpress-24"},
	expand:  "wordpress",
	expect:  []string{"cs:precise/wordpress-23", "cs:precise/wordpress-24"},
}, {
	inStore: []string{"cs:precise/wordpress-23", "cs:trusty/wordpress-24"},
	expand:  "precise/wordpress",
	expect:  []string{"cs:precise/wordpress-23"},
}, {
	inStore: []string{"cs:precise/wordpress-23", "cs:trusty/wordpress-24", "cs:foo/bar-434"},
	expand:  "wordpress",
	expect:  []string{"cs:precise/wordpress-23", "cs:trusty/wordpress-24"},
}, {
	inStore: []string{"cs:precise/wordpress-23", "cs:trusty/wordpress-23", "cs:trusty/wordpress-24"},
	expand:  "wordpress-23",
	expect:  []string{"cs:precise/wordpress-23", "cs:trusty/wordpress-23"},
}, {
	inStore: []string{"cs:~user/precise/wordpress-23", "cs:~user/trusty/wordpress-23"},
	expand:  "~user/precise/wordpress",
	expect:  []string{"cs:~user/precise/wordpress-23"},
}, {
	inStore: []string{"cs:~user/precise/wordpress-23", "cs:~user/trusty/wordpress-23"},
	expand:  "~user/wordpress",
	expect:  []string{"cs:~user/precise/wordpress-23", "cs:~user/trusty/wordpress-23"},
}, {
	inStore: []string{"cs:precise/wordpress-23", "cs:trusty/wordpress-24", "cs:foo/bar-434"},
	expand:  "precise/wordpress-23",
	expect:  []string{"cs:precise/wordpress-23"},
}, {
	inStore: []string{"cs:precise/wordpress-23", "cs:trusty/wordpress-24", "cs:foo/bar-434"},
	expand:  "arble",
	expect:  []string{},
}}

func (s *StoreSuite) TestExpandURL(c *gc.C) {
	s.testURLFinding(c, func(store *Store, expand *charm.Reference, expect []*charm.Reference) {
		gotURLs, err := store.ExpandURL(expand)
		c.Assert(err, gc.IsNil)

		sort.Sort(orderedURLs(gotURLs))
		c.Assert(gotURLs, jc.DeepEquals, expect)
	})
}

func (s *StoreSuite) testURLFinding(c *gc.C, check func(store *Store, expand *charm.Reference, expect []*charm.Reference)) {
	wordpress := testing.Charms.CharmDir("wordpress")
	for i, test := range urlFindingTests {
		c.Logf("test %d: %q from %q", i, test.expand, test.inStore)
		store, err := NewStore(s.Session.DB("foo"))
		c.Assert(err, gc.IsNil)
		_, err = store.DB.Entities().RemoveAll(nil)
		c.Assert(err, gc.IsNil)
		urls := mustParseReferences(test.inStore)
		for _, url := range urls {
			err := store.AddCharmWithArchive(url, wordpress)
			c.Assert(err, gc.IsNil)
		}
		expectURLs := make([]*charm.Reference, len(test.expect))
		for i, expect := range test.expect {
			expectURLs[i] = mustParseReference(expect)
		}
		check(store, mustParseReference(test.expand), expectURLs)
	}
}

func (s *StoreSuite) TestFindEntities(c *gc.C) {
	s.testURLFinding(c, func(store *Store, expand *charm.Reference, expect []*charm.Reference) {
		// check FindEntities works when just retrieving the id.
		gotEntities, err := store.FindEntities(expand, "_id")
		c.Assert(err, gc.IsNil)
		sort.Sort(entitiesByURL(gotEntities))
		c.Assert(gotEntities, gc.HasLen, len(expect))
		for i, url := range expect {
			c.Assert(gotEntities[i], jc.DeepEquals, &mongodoc.Entity{
				URL: url,
			})
		}

		// check FindEntities works when retrieving all fields.
		gotEntities, err = store.FindEntities(expand)
		c.Assert(err, gc.IsNil)
		sort.Sort(entitiesByURL(gotEntities))
		c.Assert(gotEntities, gc.HasLen, len(expect))
		for i, url := range expect {
			var entity mongodoc.Entity
			err := store.DB.Entities().FindId(url).One(&entity)
			c.Assert(err, gc.IsNil)
			c.Assert(gotEntities[i], jc.DeepEquals, &entity)
		}
	})
}

type entitiesByURL []*mongodoc.Entity

func (s entitiesByURL) Len() int      { return len(s) }
func (s entitiesByURL) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s entitiesByURL) Less(i, j int) bool {
	return s[i].URL.String() < s[j].URL.String()
}

var bundleUnitCountTests = []struct {
	about       string
	data        *charm.BundleData
	expectUnits int
}{{
	about: "empty bundle",
	data:  &charm.BundleData{},
}, {
	about: "no units",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:utopic/django-0",
				NumUnits: 0,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-0",
				NumUnits: 0,
			},
		},
	},
}, {
	about: "a single unit",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 0,
			},
		},
	},
	expectUnits: 1,
}, {
	about: "multiple units",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:utopic/django-1",
				NumUnits: 1,
			},
			"haproxy": {
				Charm:    "cs:utopic/haproxy-2",
				NumUnits: 2,
			},
			"postgres": {
				Charm:    "cs:utopic/postgres-3",
				NumUnits: 5,
			},
		},
	},
	expectUnits: 8,
}}

func (s *StoreSuite) TestBundleUnitCount(c *gc.C) {
	store, err := NewStore(s.Session.DB("foo"))
	c.Assert(err, gc.IsNil)
	entities := store.DB.Entities()
	for i, test := range bundleUnitCountTests {
		c.Logf("test %d: %s", i, test.about)
		url := &charm.Reference{
			Schema:   "cs",
			Series:   "utopic",
			Name:     "django",
			Revision: i,
		}

		// Add the bundle used for this test.
		err := store.AddBundle(url, &testingBundle{
			data: test.data,
		}, "blobName", fakeBlobHash, fakeBlobSize)
		c.Assert(err, gc.IsNil)

		// Retrieve the bundle from the database.
		var doc mongodoc.Entity
		err = entities.FindId(url).One(&doc)
		c.Assert(err, gc.IsNil)

		c.Assert(*doc.BundleUnitCount, gc.Equals, test.expectUnits)
	}
}

var bundleMachineCountTests = []struct {
	about          string
	data           *charm.BundleData
	expectMachines int
}{{
	about: "no machines",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:utopic/django-0",
				NumUnits: 0,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-0",
				NumUnits: 0,
			},
		},
	},
}, {
	about: "a single machine (no placement)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 0,
			},
		},
	},
	expectMachines: 1,
}, {
	about: "a single machine (machine placement)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil,
		},
	},
	expectMachines: 1,
}, {
	about: "a single machine (hulk smash)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 1,
				To:       []string{"1"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil,
		},
	},
	expectMachines: 1,
}, {
	about: "a single machine (co-location)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 1,
				To:       []string{"django/0"},
			},
		},
	},
	expectMachines: 1,
}, {
	about: "a single machine (containerization)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 1,
				To:       []string{"lxc:1"},
			},
			"postgres": {
				Charm:    "cs:utopic/postgres-3",
				NumUnits: 2,
				To:       []string{"kvm:1"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil,
		},
	},
	expectMachines: 1,
}, {
	about: "multiple machines (no placement)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:utopic/django-1",
				NumUnits: 1,
			},
			"haproxy": {
				Charm:    "cs:utopic/haproxy-2",
				NumUnits: 2,
			},
			"postgres": {
				Charm:    "cs:utopic/postgres-3",
				NumUnits: 5,
			},
		},
	},
	expectMachines: 1 + 2 + 5,
}, {
	about: "multiple machines (machine placement)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:utopic/django-1",
				NumUnits: 2,
				To:       []string{"1", "3"},
			},
			"haproxy": {
				Charm:    "cs:utopic/haproxy-2",
				NumUnits: 1,
				To:       []string{"2"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil, "2": nil, "3": nil,
		},
	},
	expectMachines: 2 + 1,
}, {
	about: "multiple machines (hulk smash)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 1,
				To:       []string{"2"},
			},
			"postgres": {
				Charm:    "cs:utopic/postgres-3",
				NumUnits: 2,
				To:       []string{"1", "2"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil, "2": nil,
		},
	},
	expectMachines: 1 + 1 + 0,
}, {
	about: "multiple machines (co-location)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 2,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 3,
				To:       []string{"django/0", "django/1", "new"},
			},
		},
	},
	expectMachines: 2 + 1,
}, {
	about: "multiple machines (containerization)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 2,
				To:       []string{"1", "2"},
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 4,
				To:       []string{"lxc:1", "lxc:2", "lxc:3", "lxc:3"},
			},
			"postgres": {
				Charm:    "cs:utopic/postgres-3",
				NumUnits: 1,
				To:       []string{"kvm:2"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil, "2": nil, "3": nil,
		},
	},
	expectMachines: 2 + 1 + 0,
}, {
	about: "multiple machines (partial placement in a container)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 10,
				To:       []string{"lxc:1", "lxc:2"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil, "2": nil,
		},
	},
	expectMachines: 1 + 1,
}, {
	about: "multiple machines (partial placement in a new machine)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 1,
				To:       []string{"1"},
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 10,
				To:       []string{"lxc:1", "1", "new"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil,
		},
	},
	expectMachines: 1 + 8,
}, {
	about: "multiple machines (partial placement with new machines)",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"django": {
				Charm:    "cs:trusty/django-42",
				NumUnits: 3,
			},
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 6,
				To:       []string{"new", "1", "lxc:1", "new"},
			},
			"postgres": {
				Charm:    "cs:utopic/postgres-3",
				NumUnits: 10,
				To:       []string{"kvm:2", "lxc:django/1", "new", "new", "kvm:2"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil, "2": nil,
		},
	},
	expectMachines: 3 + 5 + 3,
}, {
	about: "placement into container on new machine",
	data: &charm.BundleData{
		Services: map[string]*charm.ServiceSpec{
			"haproxy": {
				Charm:    "cs:trusty/haproxy-47",
				NumUnits: 6,
				To:       []string{"lxc:new", "1", "lxc:1", "kvm:new"},
			},
		},
		Machines: map[string]*charm.MachineSpec{
			"1": nil,
		},
	},
	expectMachines: 5,
}}

func (s *StoreSuite) TestBundleMachineCount(c *gc.C) {
	store, err := NewStore(s.Session.DB("foo"))
	c.Assert(err, gc.IsNil)
	entities := store.DB.Entities()
	for i, test := range bundleMachineCountTests {
		c.Logf("test %d: %s", i, test.about)
		url := &charm.Reference{
			Schema:   "cs",
			Series:   "utopic",
			Name:     "django",
			Revision: i,
		}
		err := test.data.Verify(func(string) error { return nil })
		c.Assert(err, gc.IsNil)
		// Add the bundle used for this test.
		err = store.AddBundle(url, &testingBundle{
			data: test.data,
		}, "blobName", fakeBlobHash, fakeBlobSize)
		c.Assert(err, gc.IsNil)

		// Retrieve the bundle from the database.
		var doc mongodoc.Entity
		err = entities.FindId(url).One(&doc)
		c.Assert(err, gc.IsNil)

		c.Assert(*doc.BundleMachineCount, gc.Equals, test.expectMachines)
	}
}

func urlStrings(urls []*charm.Reference) []string {
	urlStrs := make([]string, len(urls))
	for i, url := range urls {
		urlStrs[i] = url.String()
	}
	return urlStrs
}

func mustParseReferences(urlStrs []string) []*charm.Reference {
	urls := make([]*charm.Reference, len(urlStrs))
	for i, u := range urlStrs {
		urls[i] = mustParseReference(u)
	}
	return urls
}

func (s *StoreSuite) TestAddCharmDir(c *gc.C) {
	charmDir := testing.Charms.CharmDir("wordpress")
	s.checkAddCharm(c, charmDir)
}

func (s *StoreSuite) TestAddCharmArchive(c *gc.C) {
	charmArchive := testing.Charms.CharmArchive(c.MkDir(), "wordpress")
	s.checkAddCharm(c, charmArchive)
}

func (s *StoreSuite) TestAddBundleDir(c *gc.C) {
	bundleDir := testing.Charms.BundleDir("wordpress")
	s.checkAddBundle(c, bundleDir)
}

func (s *StoreSuite) TestAddBundleArchive(c *gc.C) {
	bundleArchive, err := charm.ReadBundleArchive(
		testing.Charms.BundleArchivePath(c.MkDir(), "wordpress"),
	)
	c.Assert(err, gc.IsNil)
	s.checkAddBundle(c, bundleArchive)
}

func (s *StoreSuite) TestOpenBlob(c *gc.C) {
	charmArchive := testing.Charms.CharmArchive(c.MkDir(), "wordpress")

	store, err := NewStore(s.Session.DB("foo"))
	c.Assert(err, gc.IsNil)
	url := mustParseReference("cs:precise/wordpress-23")

	err = store.AddCharmWithArchive(url, charmArchive)
	c.Assert(err, gc.IsNil)

	r, size, err := store.OpenBlob(url)
	c.Assert(err, gc.IsNil)
	defer r.Close()

	f, err := os.Open(charmArchive.Path)
	c.Assert(err, gc.IsNil)
	defer f.Close()
	c.Assert(hashOfReader(c, r), gc.Equals, hashOfReader(c, f))

	info, err := f.Stat()
	c.Assert(err, gc.IsNil)
	c.Assert(size, gc.Equals, info.Size())
}

func (s *StoreSuite) TestBlobName(c *gc.C) {
	charmArchive := testing.Charms.CharmArchive(c.MkDir(), "wordpress")

	store, err := NewStore(s.Session.DB("foo"))
	c.Assert(err, gc.IsNil)
	url := mustParseReference("cs:precise/wordpress-23")

	err = store.AddCharmWithArchive(url, charmArchive)
	c.Assert(err, gc.IsNil)

	name, err := store.BlobName(url)
	c.Assert(err, gc.IsNil)

	r, _, err := store.BlobStore.Open(name)
	c.Assert(err, gc.IsNil)
	defer r.Close()

	f, err := os.Open(charmArchive.Path)
	c.Assert(err, gc.IsNil)
	defer f.Close()
	c.Assert(hashOfReader(c, r), gc.Equals, hashOfReader(c, f))
}

func (s *StoreSuite) TestCollections(c *gc.C) {
	store, err := NewStore(s.Session.DB("foo"))
	c.Assert(err, gc.IsNil)
	colls := store.DB.Collections()
	names, err := store.DB.CollectionNames()
	c.Assert(err, gc.IsNil)
	// Check that all collections mentioned by Collections are actually created.
	for _, coll := range colls {
		found := false
		for _, name := range names {
			if name == coll.Name {
				found = true
			}
		}
		if !found {
			c.Errorf("collection %q not created", coll.Name)
		}

	}
	// Check that all created collections are mentioned in Collections.
	for _, name := range names {
		if name == "system.indexes" || name == "managedStoredResources" {
			continue
		}
		found := false
		for _, coll := range colls {
			if coll.Name == name {
				found = true
			}
		}
		if !found {
			c.Errorf("extra collection %q found", name)
		}
	}
}

func hashOfReader(c *gc.C, r io.Reader) string {
	hash := sha256.New()
	_, err := io.Copy(hash, r)
	c.Assert(err, gc.IsNil)
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func mustGetSizeAndHash(c interface{}) (int64, string) {
	var r io.ReadWriter
	var err error
	switch c := c.(type) {
	case archiverTo:
		r = new(bytes.Buffer)
		err = c.ArchiveTo(r)
	case *charm.BundleArchive:
		r, err = os.Open(c.Path)
	case *charm.CharmArchive:
		r, err = os.Open(c.Path)
	default:
		panic(fmt.Sprintf("unable to get size and hash for type %T", c))
	}
	if err != nil {
		panic(err)
	}
	hash := blobstore.NewHash()
	size, err := io.Copy(hash, r)
	if err != nil {
		panic(err)
	}
	return size, fmt.Sprintf("%x", hash.Sum(nil))
}

func mustParseReference(url string) *charm.Reference {
	ref, err := charm.ParseReference(url)
	if err != nil {
		panic(err)
	}
	return ref
}

// testingBundle implements charm.Bundle, allowing tests
// to create a bundle with custom data.
type testingBundle struct {
	data *charm.BundleData
}

func (b *testingBundle) Data() *charm.BundleData {
	return b.data
}

func (b *testingBundle) ReadMe() string {
	// For the purposes of this implementation, the charm readme is not
	// relevant.
	return ""
}

// Define fake blob attributes to be used in tests.
var fakeBlobSize, fakeBlobHash = func() (int64, string) {
	b := []byte("fake content")
	h := blobstore.NewHash()
	h.Write(b)
	return int64(len(b)), fmt.Sprintf("%x", h.Sum(nil))
}()

var exportTestCharms = map[string]string{"wordpress": "cs:precies/wordpress-23", "mysql": "cs:precise/mysql-42"}

func (s *StoreSuite) TestSuccessfulExport(c *gc.C) {
	store, err := NewStore(s.Session.DB("mongodoctoelasticsearch"))
	c.Assert(err, gc.IsNil)
	store.ES = StoreElasticSearch{s.ES, true}
	c.Assert(err, gc.IsNil)
	s.addCharmsToStore(store)
	err = store.ExportToElasticSearch()
	c.Assert(err, gc.IsNil)

	var expected mongodoc.Entity
	var actual mongodoc.Entity
	for _, ref := range exportTestCharms {
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

func (s *StoreSuite) addCharmsToStore(store *Store) {
	for name, ref := range exportTestCharms {
		charmArchive := testing.Charms.CharmDir(name)
		url, _ := charm.ParseReference(ref)
		store.AddCharmWithArchive(url, charmArchive)
	}
}

func (s *StoreSuite) TestSESPutIsNoopWithNoESConfigured(c *gc.C) {
	store, err := NewStore(s.Session.DB("mongodoctoelasticsearch"))
	c.Assert(err, gc.IsNil)
	var entity mongodoc.Entity
	err = store.ES.Put(&entity)
	c.Assert(err, gc.IsNil)
}
