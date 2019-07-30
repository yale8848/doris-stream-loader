// Create by Yale 2019/7/30 14:22
package doris

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

type Loader interface {
	LoadByFile(filePath string,options ...Option)(rest *Result,err error)
	LoadByDir(fileDir string,options ...Option)(rest []*Result,err error)
	LoadByReader(reader io.Reader,options ...Option)(rest *Result,err error)

    LabelState(label string,options ...Option)(rest *Result,err error)
	LabelCancel(label string,options ...Option)(rest *Result,err error)
}


type Result struct {
	TxnID                int    `json:"TxnId"`
	Label                string `json:"Label"`
	Status               string `json:"Status"`
	Message              string `json:"Message"`
	NumberTotalRows      int    `json:"NumberTotalRows"`
	NumberLoadedRows     int    `json:"NumberLoadedRows"`
	NumberFilteredRows   int    `json:"NumberFilteredRows"`
	NumberUnselectedRows int    `json:"NumberUnselectedRows"`
	LoadBytes            int    `json:"LoadBytes"`
	LoadTimeMs           int    `json:"LoadTimeMs"`
	State                string `json:"state"`
}

type reqUrl func(req *request)string
func (res *Result)StatusOk() bool  {
	return res.Status == "Success"
}

type Load struct {
	config LoadConfig
}

func New(config LoadConfig) Loader {
	return &Load{config}
}
func (load *Load)LabelCancel(label string,options ...Option)(rest *Result,err error){

	return load.request("POST", func(req *request) string {
		return req.config.urlLabel(label,"_cancel")
	},nil,options...)
}
func (load *Load)request(method string,urlValue reqUrl,body io.Reader,options ...Option)(rest *Result,err error){
	var bRes []byte
	req:=NewRequest(load.config)


	req.setOptions(options...)

	err =  req.config.check()
	if err!=nil {
		return
	}

	bRes,err = req.httpRequest(method,urlValue(req),body)
	if err!=nil {
		return
	}

	rest = new(Result)

	err = json.Unmarshal(bRes,rest)
	if err!=nil {
		return
	}
	return
}
func (load *Load)LabelState(label string,options ...Option)(rest *Result,err error){

	return load.request("GET", func(req *request) string {
		return req.config.urlLabel(label,"_state")
	},nil,options...)
}
func (load *Load)LoadByFile(filePath string,options ...Option) (rest *Result,err error) {

	var data []byte

	data,err=ioutil.ReadFile(filePath)
	if err!=nil {
		return
	}
	return load.LoadByReader(bytes.NewReader(data),options...)

}
func (load *Load)LoadByDir(fileDir string,options ...Option)  (rest []*Result,err error){
	rest = make([]*Result,0)
	err=filepath.Walk(fileDir, func(path string, info os.FileInfo, err error) error {

		if !info.IsDir() {
			res,err:=load.LoadByFile(path,options...)
			if err!=nil {
				return err
			}
			rest = append(rest,res)
			if !res.StatusOk() {
				return errors.New(res.Message)
			}
		}
		return nil
	})
	return
}
func (load *Load)LoadByReader(reader io.Reader,options ...Option) (rest *Result,err error) {

	return load.request("PUT", func(req *request) string {
		return req.config.url("_stream_load")
	},reader,options...)

}