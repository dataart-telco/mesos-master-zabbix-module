package main

import (
	"encoding/json"
	"errors"
	"github.com/dataart-telco/g2z"
	"github.com/scalingdata/gcfg"
	"io"
	"io/ioutil"
	"os"
)

const DATA_FILE = "/tmp/messo_agent_data.json"

type Config struct {
	Main struct {
		LogLevel          string
		CollectorInterval int
	}

	Mesos struct {
		Host string
	}
}

type ZabbixAgent struct {
}

type Segment []Slave

func (self Segment) GetResources() (Resources, Resources) {
	cpuAll := 0.0
	memAll := 0.0
	cpuUsed := 0.0
	memUsed := 0.0
	for _, n := range self {
		cpuAll += n.Resources.Cpus
		memAll += n.Resources.Mem

		cpuUsed += n.UsedResources.Cpus
		memUsed += n.UsedResources.Mem
	}
	return Resources{Cpus: cpuAll, Mem: memAll}, Resources{Cpus: cpuUsed, Mem: memUsed}
}

func (self *ZabbixAgent) OnStateCollected(data *MasterState) {
	toFile(data)
}

func toFile(data *MasterState) {
	bytes, _ := json.Marshal(data)
	err := ioutil.WriteFile(DATA_FILE, bytes, 0777)
	if err != nil {
		Error.Println("can not write data file", err)
	}
}

func fromFile() (*MasterState, error) {
	bytes, err := ioutil.ReadFile(DATA_FILE)
	if err != nil {
		Error.Println("can not read data file", err)
		return nil, err
	}
	var data MasterState
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		Error.Println("can not decode file", err)
		return nil, err
	}
	return &data, nil
}

func (self *ZabbixAgent) SegmentDiscovery(request *g2z.AgentRequest) (g2z.DiscoveryData, error) {
	Trace.Println("SegmentDiscovery")
	state, err := fromFile()
	if err != nil {
		Error.Print("Discovery error:", err)
		return nil, err
	}

	segments := make(map[string]bool)

	for _, i := range state.Slaves {
		if i.Attributes == nil {
			continue
		}
		if val, ok := i.Attributes["segment"]; ok {
			if val == nil {
				continue
			}
			if segment, ok := val.(string); ok {
				segments[segment] = true
			}
		}
	}

	discovery := make(g2z.DiscoveryData, 0, len(segments))
	for key, _ := range segments {
		item := make(g2z.DiscoveryItem)
		item["SEGMENT_ID"] = key
		discovery = append(discovery, item)
	}
	Trace.Println("Discovery result:", discovery)
	return discovery, nil
}

func (self *ZabbixAgent) GetSegment(request *g2z.AgentRequest) (Segment, error) {
	Trace.Println("GetSegment")
	if len(request.Params) < 1 {
		Error.Print("invalid params count")
		return nil, errors.New("invalid params count: Expected 1 arg")
	}

	segmentName := request.Params[0]
	state, err := fromFile()
	if err != nil {
		Error.Print("GetSegment error:", err)
		return nil, errors.New("Agent internal error")
	}

	nodes := make([]Slave, 0, len(state.Slaves))
	for _, i := range state.Slaves {
		if i.Attributes == nil {
			continue
		}
		if val, ok := i.Attributes["segment"]; ok {
			if val == nil {
				continue
			}
			if segment, ok := val.(string); ok && segmentName == segment {
				nodes = append(nodes, i)
			}
		}
	}
	return nodes, nil
}

func (self *ZabbixAgent) SegmentSize(request *g2z.AgentRequest) (uint64, error) {
	Trace.Println("SegmentSize")
	segment, err := self.GetSegment(request)
	if err != nil {
		return 0, err
	}
	return uint64(len(segment)), nil
}

func (self *ZabbixAgent) SegmentUsage(request *g2z.AgentRequest) (float64, float64, error) {
	Trace.Println("SegmentUsage")
	segment, err := self.GetSegment(request)
	if err != nil {
		return 0, 0, err
	}
	all, used := segment.GetResources()
	return used.Cpus/all.Cpus, used.Mem/all.Mem, nil
}

func (self *ZabbixAgent) SegmentFree(request *g2z.AgentRequest) (float64, float64, error) {
	Trace.Println("SegmentFree")
	segment, err := self.GetSegment(request)
	if err != nil {
		return 0, 0, err
	}
	all, used := segment.GetResources()
	return (all.Cpus - used.Cpus)/all.Cpus, (all.Mem - used.Mem)/all.Mem, nil
}

var zabbixAgent = &ZabbixAgent{}
var monitor *Monitor

func init() {
	cfg := &Config{}
	err := gcfg.ReadFileInto(cfg, "master-zabbix-agent.ini")
	if err != nil {
		Error.Println("can not read ini file", err)
		Info.Println("Use default settings")
		monitor = &Monitor{
			Host:              "127.0.0.1:5050",
			collectorInterval: 10,
			listener:          zabbixAgent}
	} else {
		monitor = &Monitor{
			Host:              cfg.Mesos.Host,
			collectorInterval: cfg.Main.CollectorInterval,
			listener:          zabbixAgent}

		var traceHandle io.Writer
		if cfg.Main.LogLevel == "TRACE" {
			traceHandle = os.Stdout
		} else {
			traceHandle = ioutil.Discard
		}
		InitLog(traceHandle, os.Stdout, os.Stdout, os.Stderr)
	}

	monitor.StartWorker()
	g2z.RegisterDiscoveryItem("cluster.segment.discovery", "Cluster segments", zabbixAgent.SegmentDiscovery)
	g2z.RegisterUint64Item("cluster.segment.size", "Cluster segemnt size", zabbixAgent.SegmentSize)
	g2z.RegisterDoubleItem("cluster.segment.used.cpu", "Cluster segemnt usage cpu", 
		func (request *g2z.AgentRequest) (float64, error){
			cpu, _, err := zabbixAgent.SegmentUsage(request)
			return cpu, err
		})
	g2z.RegisterDoubleItem("cluster.segment.used.mem", "Cluster segemnt usage cpu", 
		func (request *g2z.AgentRequest) (float64, error){
			_, mem, err := zabbixAgent.SegmentUsage(request)
			return mem, err
		})
	g2z.RegisterDoubleItem("cluster.segment.free.cpu", "Cluster segemnt free cpu", 
		func (request *g2z.AgentRequest) (float64, error){
			cpu, _, err := zabbixAgent.SegmentFree(request)
			return cpu, err
		})
	g2z.RegisterDoubleItem("cluster.segment.free.mem", "Cluster segemnt free cpu", 
		func (request *g2z.AgentRequest) (float64, error){
			_, mem, err := zabbixAgent.SegmentFree(request)
			return mem, err
		})

	g2z.RegisterUninitHandler(func() error {
		monitor.StopWorker()
		return nil
	})
}
