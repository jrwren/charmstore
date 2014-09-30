// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

// This file is a simplified copy of github.com/juju/testing/mgo.go

package elasticsearch

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"text/template"

	"github.com/juju/errgo"
	"github.com/juju/loggo"
	jujutesting "github.com/juju/testing"

	gc "gopkg.in/check.v1"
)

type ElasticSearchInstance struct {
	Dir      string
	HTTPPort int
	Server   *exec.Cmd
	exited   <-chan struct{}
}

var (
	elasticSearchServer = &ElasticSearchInstance{}
	logger              = loggo.GetLogger("juju.testing.elasticsearchsuite")
)

const (
	maxStartAttempts = 5

	// ElasticSearch exits with code 143 on SIGTERM.
	elasticSearchSigTermErrCode = 143
)

// Tests can be run using already running elasticsearch:
// $ JUJU_TEST_ELASTICSEARCH=1234 go test -v ./internal/elasticsearch -gocheck.v
// Tests can be disabled:
// JUJU_TEST_ELASTICSEARCH=none go test -v ./internal/elasticsearch -gocheck.v
// Tests can start an elasticsearch instance themselves (this take 6 seconds):
// JUJU_TEST_ELASTICSEARCH= go test -v ./internal/elasticsearch -gocheck.v
func ElasticSearchTestPackage(t *testing.T, cb func(t *testing.T)) {
	if os.Getenv("JUJU_TEST_ELASTICSEARCH") == "none" {
		return
	}
	if os.Getenv("JUJU_TEST_ELASTICSEARCH") == "" {
		if err := elasticSearchServer.Start(); err != nil {
			t.Fatal(err)
		}
		defer elasticSearchServer.Destroy()
	} else {
		var err error
		elasticSearchServer.HTTPPort, err = strconv.Atoi(os.Getenv("JUJU_TEST_ELASTICSEARCH"))
		if err != nil {
			panic("invalid JUJU_TEST_ELASTICSEARCH value. expect an valid tcp port or none")
		}
	}
	if cb != nil {
		cb(t)
	} else {
		gc.TestingT(t)
	}
}

func (es *ElasticSearchInstance) kill(sig syscall.Signal) {
	es.Server.Process.Signal(sig)
	<-es.exited
	es.Server = nil
	es.exited = nil
}

var config = template.Must(template.New("").Parse(`
path.data: {{.Dir}}/data
path.logs: {{.Dir}}/log/
network.host: 127.0.0.1
http.port: {{.HTTPPort}}
`))

func (es *ElasticSearchInstance) run() error {
	if es.Server != nil {
		panic("elasticsearch is already running")
	}
	configFile, err := es.writeConfig()
	if err != nil {
		return err
	}
	server := exec.Command("elasticsearch", "--config="+configFile)
	out, err := server.StdoutPipe()
	if err != nil {
		return err
	}
	exited := make(chan struct{})
	started := make(chan error)
	listening := make(chan error, 1)
	go func() {
		err := <-started
		if err != nil {
			close(listening)
			close(exited)
			return
		}
		var buf bytes.Buffer
		prefix := fmt.Sprintf("inet[/127.0.0.1:%v", es.HTTPPort)
		if readUntilMatching(prefix, io.TeeReader(out, &buf), regexp.MustCompile("node.*started")) {
			listening <- nil
		} else {
			err := errgo.Newf("elasticsearch failed to listen on port %v using config %v", es.HTTPPort, configFile)
			if strings.Contains(buf.String(), "Address already in use") {
				err = addrAlreadyInUseError{err}
			}
			listening <- err
		}
		lines := readLastLines(prefix, io.MultiReader(&buf, out), 20)
		err = server.Wait()
		exitErr, _ := err.(*exec.ExitError)
		defer close(exited)
		if exitErr != nil {
			exitCode := exitErr.Sys().(syscall.WaitStatus).ExitStatus()
			if exitCode == elasticSearchSigTermErrCode {
				return
			}
		}
		if err != nil || exitErr != nil && exitErr.Exited() {
			logger.Errorf("elasticsearch has exited without being killed")
			for _, line := range lines {
				logger.Errorf("elasticsearch: %s", line)
			}
		}
	}()
	es.exited = exited
	err = server.Start()
	started <- err
	if err != nil {
		return err
	}
	err = <-listening
	close(listening)
	if err != nil {
		return err
	}
	es.Server = server
	return nil
}

func (es *ElasticSearchInstance) writeConfig() (string, error) {
	if es.Dir == "" {
		return "", errgo.New("directory not set")
	}
	file, err := os.Create(es.Dir + "/elasticsearch.yml")
	if err != nil {
		return "", err
	}
	defer file.Close()
	config.Execute(file, es)
	return file.Name(), nil
}

func (es *ElasticSearchInstance) Destroy() {
	if es.Server != nil {
		term := syscall.SIGTERM
		logger.Debugf("killing elasticsearch pid %d in %s on port %d with %s", es.Server.Process.Pid, es.Dir, es.HTTPPort, term)
		es.kill(term)
		os.RemoveAll(es.Dir)
		es.Dir = ""
		return
	}
	logger.Errorf("Destroy called when elasticsearch was not started")
}
func (es *ElasticSearchInstance) Start() error {
	dir, err := ioutil.TempDir("", "test-es")
	if err != nil {
		return err
	}
	es.Dir = dir
	logger.Debugf("starting elasticsearch in ", es.Dir)
	for i := 0; i < maxStartAttempts; i++ {
		es.HTTPPort = jujutesting.FindTCPPort()
		err = es.run()
		switch err.(type) {
		case addrAlreadyInUseError:
			logger.Debugf("failed to start elasticssearch: %v, trying another port", err)
			continue
		case nil:
			logger.Debugf("started elasticsearch pid %d in %s on port %d", es.Server.Process.Pid, es.Dir, es.HTTPPort)
		default:
			es.HTTPPort = 0
			os.RemoveAll(es.Dir)
			es.Dir = ""
			logger.Warningf("failed to start elasticsearch %v", err)
		}
		break
	}
	return err
}

func (es *ElasticSearchInstance) dropAll(db *Database) error {
	//for index in curl 'localhost:9200/_cat/indices?v'
	// delete index
	names, err := db.ListAllIndexes()
	if err != nil {
		return err
	}
	for _, name := range names {
		db.DeleteIndex(name)
	}
	return nil
}

type ElasticSearchSuite struct {
	*ElasticSearchInstance
	ES *Database
}

func (s *ElasticSearchSuite) SetUpSuite(c *gc.C) {
	s.ElasticSearchInstance = elasticSearchServer
	s.ES = &Database{"127.0.0.1", elasticSearchServer.HTTPPort}
}

func (s *ElasticSearchSuite) TearDownSuite(c *gc.C) {
}

func (s *ElasticSearchSuite) SetUpTest(c *gc.C) {
	s.dropAll(s.ES)
}

func (s *ElasticSearchSuite) TearDownTest(c *gc.C) {
}

func readLastLines(prefix string, r io.Reader, n int) []string {
	sc := bufio.NewScanner(r)
	lines := make([]string, n)
	i := 0
	for sc.Scan() {
		if line := strings.TrimRight(sc.Text(), "\n"); line != "" {
			logger.Tracef("%s: %s", prefix, line)
			lines[i%n] = line
			i++
		}
	}
	if err := sc.Err(); err != nil {
		panic(err)
	}
	final := make([]string, 0, n+1)
	if i > n {
		final = append(final, fmt.Sprintf("[%d lines omitted]", i-n))
	}
	for j := 0; j < n; j++ {
		if line := lines[(j+i)%n]; line != "" {
			final = append(final, line)
		}
	}
	return final
}
func readUntilMatching(prefix string, r io.Reader, re *regexp.Regexp) bool {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := sc.Text()
		logger.Tracef("%s: %s", prefix, line)
		if re.MatchString(line) {
			return true
		}
	}
	return false
}

type addrAlreadyInUseError struct {
	error
}
