package main

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func Post(url string, data string) (int, error) {
	Trace.Println("Make POST req: url =", url, "with body ", data)

	client := &http.Client{}
	r, _ := http.NewRequest(
		"POST",
		url,
		bytes.NewBufferString(data))

	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Add("Content-Length", strconv.Itoa(len(data)))
	r.Close = true
	resp, err := client.Do(r)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return resp.StatusCode, errors.New("Resp code is not 200 for " + url + "; status = " + resp.Status)
	}
	return 200, nil
}

func Get(url string) (int, []byte, error) {
	Trace.Println("Make Get req: url =", url)
	client := &http.Client{}
	r, _ := http.NewRequest(
		"GET",
		url,
		nil)
	r.Close = true
	resp, err := client.Do(r)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return resp.StatusCode, nil, errors.New("Resp code is not 200 for " + url + "; status = " + resp.Status)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return 200, body, nil
}

func WaitCtrlC() {
	var signal_channel chan os.Signal
	signal_channel = make(chan os.Signal, 2)
	signal.Notify(signal_channel, os.Interrupt, syscall.SIGTERM)
	<-signal_channel
}

func schedule(step int, what func()) chan int {
	ticker := time.NewTicker(time.Duration(step) * time.Second)
	quit := make(chan int)
	go func() {
		for {
			select {
			case <-quit:
				return
			case <-ticker.C:
				what()
			}
		}
	}()
	return quit
}
