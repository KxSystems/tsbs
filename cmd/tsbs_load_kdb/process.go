package main

import (
	"log"

	"github.com/questdb/tsbs/pkg/targets"
	kdb "github.com/sv/kdbgo"
)

var (
	qProc *kdb.KDBConn
)

func globalInit(numWorker int, _, _ bool) {
	log.Print("kdb_processor: Init")
	//Open connection to q process
	con, err := kdb.DialKDB("localhost", 5000, "")
	qProc = con
	if err != nil {
		log.Fatal("Could not connect to KDB")
	}
	log.Print("kdb_processor: Created qProc")
	_, err = qProc.Call("init", kdb.Int(0))
	if err != nil {
		log.Fatalf("Error init: %s\n", err.Error())
	}
}

func globalClose(_ bool) {
	log.Println("Close ingest")
	_, err := qProc.Call("finish", kdb.Int(0))
	if err != nil {
		log.Fatalf("Error closing: %s\n", err.Error())
	}
}

// Processor for string data format
type processorString struct {
	useInflux bool
	endPoint  string
}

func (p *processorString) Init(numWorker int, doLoad, hashWorkers bool) {
	globalInit(numWorker, doLoad, hashWorkers)
	if p.useInflux {
		p.endPoint = "updInflux"
	} else {
		p.endPoint = "updSrcPrefixedStrings"
	}
	log.Print("kdb_processor: Init complete")
}

func (p *processorString) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)

	if doLoad {
		_, err := qProc.Call(p.endPoint, toQStringList(batch.data))
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}
	}

	metricCount := batch.metricCount
	rowCount := uint64(batch.Len())

	return metricCount, rowCount
}

func (p *processorString) Close(doLoad bool) {
	globalClose(doLoad)
}

func toQStringList(data [][]byte) *kdb.K {
	kList := make([]*kdb.K, len(data))
	for ind, item := range data {
		kList[ind] = &kdb.K{Type: 10, Attr: kdb.NONE, Data: string(item)}
	}
	return kdb.NewList(kList...)
}

// Processor for binary data format
type processorBinary struct {
}

func (p *processorBinary) Init(numWorker int, doLoad, hashWorkers bool) {
	globalInit(numWorker, doLoad, hashWorkers)
	log.Print("kdb_processor: Init complete")
}

func (p *processorBinary) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batchBinary)

	if doLoad {
		_, err := qProc.Call("updQBinary", kdb.NewList(batch.qData...))
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}
	}

	metricCount := uint64(batch.metricCount)
	rowCount := uint64(batch.Len())

	return metricCount, rowCount
}

func (p *processorBinary) Close(doLoad bool) {
	globalClose(doLoad)
}

// Processor for binary data format
type processorBinaryDict struct {
}

func (p *processorBinaryDict) Init(numWorker int, doLoad, hashWorkers bool) {
	globalInit(numWorker, doLoad, hashWorkers)
	log.Print("kdb_processor: Init complete")
}

func (p *processorBinaryDict) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batchBinaryDict)

	if doLoad {
		_, err := qProc.Call("updQBinaryMap", dictToQDict(batch.qData))
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}
	}

	metricCount := uint64(batch.metricCount)
	rowCount := uint64(batch.Len())

	return metricCount, rowCount
}

func (p *processorBinaryDict) Close(doLoad bool) {
	globalClose(doLoad)
}

func dictToQDict(data map[string][]*kdb.K) *kdb.K {
	keys := make([]string, len(data))
	values := make([]*kdb.K, len(data))
	ind := 0
	for key, value := range data {
		keys[ind] = key
		values[ind] = kdb.NewList(value...)
		ind++
	}
	return kdb.NewDict(kdb.SymbolV(keys), kdb.NewList(values...))
}
