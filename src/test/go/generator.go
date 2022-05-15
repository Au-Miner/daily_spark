package main //包名

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func Get(url string) string {
	//tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	//client := &http.Client{Transport: tr}
	client := &http.Client{}
	resp, err := client.Get(url)
	if err != nil {
		log.Fatal(err)
	}

	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	return string(b)
}

func main() {
	fmt.Printf(Get("http://10.10.43.44:8080/api/group/userDoGroupPlan?userId=1&planId=31"))
}
