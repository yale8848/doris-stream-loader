// Create by Yale 2019/7/30 14:25
package doris

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"
)

const formatUrlTemp = `%s://%s/api/%s/%s/%s`

var (
	ErrConfigHostEmpty   = errors.New("host is empty")
	ErrConfigDBNameEmpty = errors.New("db name is empty")
)

var (
	httpClient    = &http.Client{}
	httpTransport *http.Transport
)

type Option func(req *request)

type LoadConfig struct {
	IsHttps   bool
	Host      string
	DBName    string
	TableName string
	User      string
	Password  string

	HttpTransport *http.Transport

	ShowDebug bool
}

func (lc *LoadConfig) check() error {
	if len(lc.Host) == 0 {
		return ErrConfigHostEmpty
	}
	if len(lc.DBName) == 0 {
		return ErrConfigDBNameEmpty
	}
	if len(lc.User) == 0 {
		lc.User = "root"
		lc.printf("user default is root")
	}
	return nil
}
func (lc *LoadConfig) getScheme() string {
	if lc.IsHttps {
		return "https"
	}
	return "http"
}
func (lc *LoadConfig) printf(format string, a ...interface{}) {
	if lc.ShowDebug {
		fmt.Printf("doris# "+format+"\r\n", a)
	}

}
func (lc *LoadConfig) url(action string) string {
	return fmt.Sprintf(formatUrlTemp, lc.getScheme(), lc.Host, lc.DBName, lc.TableName, action)
}
func (lc *LoadConfig) urlLabel(label, action string) string {
	return fmt.Sprintf(formatUrlTemp, lc.getScheme(), lc.Host, lc.DBName, label, action)
}

type request struct {
	config LoadConfig
	header map[string]string
}

func NewRequest(config LoadConfig) *request {
	req := &request{config: config}
	req.header = make(map[string]string)
	return req

}
func (rq *request) setOptions(options ...Option) {
	for _, v := range options {
		v(rq)
	}
}
func (rq *request) httpDial(ctx context.Context, network, addr string) (net.Conn, error) {
	conn, err := net.DialTimeout(network, addr, time.Second*30)
	if err != nil {
		return conn, err
	}
	return conn.(*net.TCPConn), err
}
func (rq *request) getHttpClient() *http.Client {

	if httpTransport == nil {
		proxy := func(req *http.Request) (*url.URL, error) {
			req.SetBasicAuth(rq.config.User, rq.config.Password)
			return nil, nil
		}
		if rq.config.HttpTransport != nil {
			httpTransport = rq.config.HttpTransport

			if httpTransport.Proxy != nil {
				tmp := httpTransport.Proxy
				httpTransport.Proxy = func(req *http.Request) (*url.URL, error) {
					req.SetBasicAuth(rq.config.User, rq.config.Password)
					return tmp(req)
				}
			} else {
				httpTransport.Proxy = proxy
			}
		} else {
			httpTransport = &http.Transport{
				DialContext: rq.httpDial,
				Proxy:       proxy,
			}
		}
		httpClient.Transport = httpTransport
	}

	return httpClient
}
func (rq *request) httpRequest(method string, urlValue string, body io.Reader) (res []byte, err error) {

	var httpReq *http.Request
	var httpRes *http.Response

	rq.config.printf("url: %s  header: %+v", urlValue, rq.header)

	httpReq, err = http.NewRequest(method, urlValue, body)
	if err != nil {
		return
	}
	httpReq.Header.Set("Expect", "100-continue")

	for k, v := range rq.header {
		httpReq.Header.Set(k, v)
	}

	httpRes, err = rq.getHttpClient().Do(httpReq)
	if err != nil {
		return
	}
	if httpRes.Body != nil {
		defer httpRes.Body.Close()
		res, _ = ioutil.ReadAll(httpRes.Body)
	}

	if httpRes.StatusCode != 200 {
		err = errors.New(fmt.Sprintf("StatusCode: %d\r\nBody: %s", httpRes.StatusCode, string(res)))
		return
	}
	return

}

func WithTableName(tableName string) Option {
	return func(req *request) {
		req.config.TableName = tableName
	}
}

func WithDBName(dbName string) Option {
	return func(req *request) {
		req.config.DBName = dbName
	}
}
func WithCustomHeader(key, value string) Option {
	return func(req *request) {
		req.header[key] = value
	}
}
func WithColumnSeparator(columnSeparator string) Option {
	return func(req *request) {
		req.header["column_separator"] = columnSeparator
	}
}
func WithWhere(where string) Option {
	return func(req *request) {
		req.header["where"] = where
	}
}
func WithMaxFilterRatio(maxFilterRatio string) Option {
	return func(req *request) {
		req.header["max_filter_ratio"] = maxFilterRatio
	}
}

func WithColumns(columns string) Option {
	return func(req *request) {
		req.header["columns"] = columns
	}
}
func WithPartitions(partitions string) Option {
	return func(req *request) {
		req.header["partitions"] = partitions
	}
}

func WithLabel(label string) Option {
	return func(req *request) {
		req.header["label"] = label
	}
}
