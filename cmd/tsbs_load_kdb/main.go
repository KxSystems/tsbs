package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"sync"

	"github.com/blagojts/viper"
	"github.com/questdb/tsbs/internal/utils"
	"github.com/questdb/tsbs/load"
	"github.com/questdb/tsbs/pkg/targets"
	"github.com/questdb/tsbs/pkg/targets/kdb"
	"github.com/spf13/pflag"
)

var (
	loader     load.BenchmarkRunner
	config     load.BenchmarkRunnerConfig
	bufPool    sync.Pool
	target     targets.ImplementedTarget
	useInflux  bool
	dataFormat string
)

// allows for testing
var fatal = log.Fatalf

func init() {
	log.Printf("main: init")
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)
	target = kdb.NewTarget()
	target.TargetSpecificFlags("", pflag.CommandLine)

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	useInflux = viper.GetBool("influx-format")
	dataFormat = viper.GetString("data-format")

	loader = load.GetBenchmarkRunner(config)
}

type benchmark struct{}

func (b *benchmark) GetDataSource() targets.DataSource {
	return &fileDataSource{scanner: bufio.NewScanner(load.GetBufferedReader(config.FileName))}
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{dataFormat: dataFormat, useInflux: useInflux}
}

func (b *benchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	switch dataFormat {
	case "string":
		return &processorString{useInflux: useInflux}
	case "binary":
		return &processorBinary{}
	case "binaryDict":
		return &processorBinaryDict{}
	default:
		errorMsg := fmt.Sprintf("Invalid data format: %s", dataFormat)
		panic(errorMsg)
	}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}

func main() {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	log.Printf("main: Running benchmark")
	loader.RunBenchmark(&benchmark{})
	log.Printf("main: Finished running benchmark")
}
