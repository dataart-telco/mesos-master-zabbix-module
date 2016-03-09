package main

import (
	"encoding/json"
)

type Resources struct {
	Cpus int `json: "cpus"`
	Disk int `json: "disk"`
	Mem  int `json: "mem"`
}
type Slave struct {
	Id            string                 `json: "id"`
	Hostname      string                 `json: "hostname"`
	Active        bool                   `json: "active"`
	Attributes    map[string]interface{} `json: "attributes"`
	Resources     Resources              `json: "resources"`
	UsedResources Resources              `json: "used_resources"`
}

type MasterState struct {
	Slaves []Slave `json: "slaves"`
}

type Monitor struct {
	Host     string
	listener StateListener

	collectorInterval int
	stopWorker        chan int
}

type StateListener interface {
	OnStateCollected(state *MasterState)
}

func (self *Monitor) CollectState() {
	_, body, err := Get("http://" + self.Host + "/master/state.json")
	if err != nil {
		Error.Println("Get master state error:", err)
		return
	}
	var masterState MasterState
	err = json.Unmarshal(body, &masterState)
	if err != nil {
		Error.Println("Can not decode json:", err)
		return
	}
	self.listener.OnStateCollected(&masterState)
}

func (self *Monitor) StartWorker() {
	Trace.Println("StartWorker:", self.collectorInterval)
	do := func() {
		self.CollectState()
	}
	self.stopWorker = schedule(self.collectorInterval, do)
	go do()
}

func (self *Monitor) StopWorker() {
	Trace.Println("StopWorker")
	if self.stopWorker == nil {
		return
	}
	self.stopWorker <- 1
	self.stopWorker = nil
}

func main() {
	panic("Should not be called")
}
