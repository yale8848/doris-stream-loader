## [Apache Doris](https://github.com/apache/incubator-doris) Stream Loader Golang API

 [Stream-Load-Manual](https://github.com/apache/incubator-doris/wiki/Stream-Load-Manual)  user curl command to load data, [doris-stream-loader](https://github.com/yale8848/doris-stream-loader.git) with Golang API code.
 
 ### Install
 
 ```bash
    go get github.com/yale8848/doris-stream-loader@v0.1.3
```
 
 ### API
 
 - Init
 
  ```go
    ld:= doris.New(doris.LoadConfig{Host:"172.16.1.244:8030",DBName:"db_dxh",TableName:"dxh_log2",User:"root",Password:"123456"})

  ```
 
 - Stream Load
 
 ```go
    res,err:=ld.LoadByFile("data.txt",doris.WithLabel("001"),
		doris.WithColumnSeparator(","))
	CheckErr(err)
	fmt.Printf("%+v\r\n",res)
```
 
 - GET LABEL STATE
 
 ```go

    res,err:=ld.LabelState("001")
    CheckErr(err)
    fmt.Printf("%+v\r\n",res)

```
 - CANCEL LABEL
 
 
 ```go
    res,err:=ld.LabelCancel("001")
    CheckErr(err)
    fmt.Printf("%+v\r\n",res)

```
 