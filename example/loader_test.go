// Create by Yale 2019/7/30 16:29
package example

import (
	"context"
	"fmt"
	"github.com/yale8848/doris-stream-loader/doris"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}
func TestLoad_LoadByFile(t *testing.T) {

	ld := doris.New(doris.LoadConfig{Host: "172.16.1.244:8030", DBName: "db_dxh", TableName: "dxh_log2",
		User: "root", Password: "123456", ShowDebug: true})
	res, err := ld.LoadByFile("data.txt", doris.WithLabel("0001"),
		doris.WithColumnSeparator(","))
	CheckErr(err)
	fmt.Printf("%+v\r\n", res)

}

func TestLoad_LabelState(t *testing.T) {
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(network, addr, time.Second*30)
		if err != nil {
			return conn, err
		}
		return conn.(*net.TCPConn), err
	}
	httpTransport := &http.Transport{
		DialContext:     dial,
		MaxConnsPerHost: 100,
		Proxy: func(request *http.Request) (url *url.URL, e error) {
			return nil, nil
		},
	}

	ld := doris.New(doris.LoadConfig{
		IsHttps: false, Host: "172.16.1.244:8030", DBName: "db_dxh",
		TableName: "dxh_log2", User: "root", Password: "123456",
		HttpTransport: httpTransport})
	res, err := ld.LabelState("0001")
	CheckErr(err)
	fmt.Printf("%+v\r\n", res)
}
func TestLoad_LabelCancel(t *testing.T) {
	ld := doris.New(doris.LoadConfig{Host: "172.16.1.244:8030", DBName: "db_dxh", TableName: "dxh_log2", User: "root", Password: "123456"})
	res, err := ld.LabelCancel("0001")
	CheckErr(err)
	fmt.Printf("%+v\r\n", res)
}
