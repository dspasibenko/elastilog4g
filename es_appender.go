package elastilog4g

import (
	"errors"
	"github.com/jrivets/log4g"
	elastigo "github.com/mattbaird/elastigo/lib"
	"strconv"
	"strings"
)

// log4g the appender registration name
const esAppenderName = "log4g/elastilog4g"

// retry - specifies number of seconds to repeat indexing attempt in case of error
// the parameter is OPTIONAL, default value is 1 second.
const ESAParamRetry = "retry"

// index - specifies ES index name. Must be provided
const ESAParamIndexName = "index"

// index - specifies ES type name. Must be provided
const ESAParamTypeName = "_type"

// Specifies ES hosts the appender will connect to. Should be comma separated like:
// "192.168.1.1, 192.168.1.2"
// the parameter is OPTIONAL, default value is localhost
const ESAParamHosts = "hosts"

// Specifies ES port the appender will connect to
// the parameter is OPTIONAL, default value is 9200
const ESAParamPort = "port"

// Specifies the record TTL
// the parameter is OPTIONAL, default value is ""
const ESAParamTTL = "ttl"

type esAppender struct {
	conn     *elastigo.Conn
	indexer  *elastigo.BulkIndexer
	index    string
	typeName string
	ttl      string
}

type esAppenderFactory struct {
}

func Init() error {
	return log4g.RegisterAppender(&esAppenderFactory{})
}

// -------- Factory functions --------

func (*esAppenderFactory) Name() string {
	return esAppenderName
}

func (*esAppenderFactory) NewAppender(params map[string]string) (log4g.Appender, error) {
	retrySec, err := log4g.ParseInt(params[ESAParamRetry], 0, 60, 1)
	if err != nil {
		return nil, errors.New("Invalid " + ESAParamRetry + " value: " + err.Error())
	}

	index := strings.Trim(params[ESAParamIndexName], " ")
	if len(index) == 0 {
		return nil, errors.New("Mandatory appender index name setting should be provided")
	}

	typeName := strings.Trim(params[ESAParamTypeName], " ")
	if len(typeName) == 0 {
		return nil, errors.New("Mandatory appender index type setting should be provided")
	}

	hosts := strings.Split(params[ESAParamHosts], ",")
	for idx, host := range hosts {
		hosts[idx] = strings.Trim(host, " ")
	}

	port, err := log4g.ParseInt(params[ESAParamPort], 1000, 65535, 9200)
	if err != nil {
		return nil, errors.New("Invalid " + ESAParamPort + " value: " + err.Error())
	}

	ttl := params[ESAParamTTL]

	conn := elastigo.NewConn()
	if len(hosts) > 0 {
		conn.ClusterDomains = hosts
	}
	conn.Port = strconv.Itoa(port)
	esa := &esAppender{conn, conn.NewBulkIndexerErrors(1, int(retrySec)), index, typeName, ttl}
	esa.indexer.Start()

	return esa, nil
}

func (f *esAppenderFactory) Shutdown() {
}

// -------- Appender functions --------
func (esa *esAppender) Append(ev *log4g.LogEvent) (ok bool) {
	ok = false
	defer log4g.EndQuietly()
	esa.indexer.Index(esa.index, esa.typeName, "", esa.ttl, &ev.Timestamp, ev, false)
	ok = true
	return ok
}

func (esa *esAppender) Shutdown() {
	esa.indexer.Stop()
	esa.conn.Flush()
}
