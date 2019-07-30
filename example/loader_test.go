// Create by Yale 2019/7/30 16:29
package example

import (
	"fmt"
	"github.com/yale8848/doris-stream-load/doris"
	"testing"
)

func CheckErr(err error)  {
	if err!=nil {
		panic(err)
	}
}
func TestLoad_LoadByFile(t *testing.T) {

	ld:= doris.New(doris.LoadConfig{Host:"172.16.1.244:8030",DBName:"db_dxh",TableName:"dxh_log2",User:"root",Password:"123456"})
	res,err:=ld.LoadByFile("data.txt",doris.WithLabel("001"),
		doris.WithColumnSeparator(","))
	CheckErr(err)
	fmt.Printf("%+v\r\n",res)

}

func TestLoad_LabelState(t *testing.T) {
	ld:= doris.New(doris.LoadConfig{IsHttps:false,Host:"172.16.1.244:8030",DBName:"db_dxh",TableName:"dxh_log2",User:"root",Password:"123456"})
	res,err:=ld.LabelState("001")
	CheckErr(err)
	fmt.Printf("%+v\r\n",res)
}
func TestLoad_LabelCancel(t *testing.T) {
	ld:= doris.New(doris.LoadConfig{Host:"172.16.1.244:8030",DBName:"db_dxh",TableName:"dxh_log2",User:"root",Password:"123456"})
	res,err:=ld.LabelCancel("001")
	CheckErr(err)
	fmt.Printf("%+v\r\n",res)
}
