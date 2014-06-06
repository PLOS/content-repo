// work in progress by Jono

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"bytes"
	"mime/multipart"
)

var server string
var bucket string
var key string
var version string
var command string
var file_path string


func getContent(url string) ([]byte, error, int, string) {

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err, 0, ""
	}

	req.Header.Add("Accept", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err, 0, ""
	}
	
	defer resp.Body.Close()
	
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err, 0, ""
	}

	return body, nil, resp.StatusCode, resp.Status
}

func postContent(url string, vals url.Values) ([]byte, error, int, string) {

	req, err := http.NewRequest("POST", url, bytes.NewBufferString(vals.Encode()))
	if err != nil {
		return nil, err, 0, ""
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Accept", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err, 0, ""
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err, 0, ""
	}

	return body, nil, resp.StatusCode, resp.Status
}

func postObject(url string, vals url.Values, file File) ([]byte, error, int, string) {


}

func show_info() {
	fmt.Println("info...")
	content, err, _, _ := getContent(server + "/info")
	if err != nil {
		panic(err)
	} else {
		var dat map[string]interface{}

		if err := json.Unmarshal(content, &dat); err != nil {
			panic(err)
		}
		fmt.Println(dat)
		//fmt.Println("version : " + dat["version"].(string))
		for k, v := range dat {
			fmt.Println(k, ":", v)
		}

	}
}

func show_objects() {

	fmt.Println("objccts...")
	content, err, _, _ := getContent(server + "/objects")
	if err != nil {
		panic(err)
	} else {
		var dat []map[string]interface{}

		if err := json.Unmarshal(content, &dat); err != nil {
			panic(err)
		}
		for _, k := range dat {
			fmt.Println(k["key"], "  size: ", k["size"])
		}

	}
}

func show_buckets() {

	fmt.Println("buckets...")
	content, err, _, _ := getContent(server + "/buckets")
	if err != nil {
		panic(err)
	} else {
		var dat []map[string]interface{}

		if err := json.Unmarshal(content, &dat); err != nil {
			panic(err)
		}
		for _, k := range dat {
			fmt.Println(k["bucketName"])
		}

	}
}

func create_bucket() {

	content, err, statusCode, _ := postContent(server+"/buckets",
		url.Values{"name": {bucket}})

	if err != nil {
		panic(err)
	} else if statusCode != 200 {
		fmt.Println("Error: ", string(content))
	} else {
		fmt.Println(string(content))
	}
}

func new_object() {

	file, err := os.Open(file_path)

	if err != nil {
		return nil, err
	}
	defer file.Close()


	content, err, statusCode, _ := postContent(server+"/objects",
		url.Values{"bucketName": {bucket}, "key": {key}, "create": {"auto"}})

	if err != nil {
		panic(err)
	} else if statusCode != 201 {
		fmt.Println("Error: ", string(content))
	} else {
		fmt.Println(string(content))
	}
}


func main() {

	// ex: go run httpTest.go -bucket bucket1 newbucket

	flag.StringVar(&server, "server", "http://localhost:8080", "repo server")
	flag.StringVar(&bucket, "bucket", "mybucket", "bucket")
	flag.Parse()

	if len(flag.Args()) == 0 {
		panic("No command set")
	}

	command := flag.Args()[0]
	fmt.Println("server: ", server)
	fmt.Println("bucket: ", bucket)
	fmt.Println("command: ", command)

	switch command {
	case "info":
		show_info()
	case "buckets":
		show_buckets()
	case "objects":
		show_objects()
	case "newbucket":
		create_bucket()
	case "newobject":
		new_object()
	default:
		fmt.Println("Command unrecognized: " + command)
	}
}
