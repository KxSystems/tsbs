package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/blagojts/viper"
	"github.com/questdb/tsbs/internal/utils"
	"github.com/questdb/tsbs/pkg/query"
	"github.com/spf13/pflag"
	kdb "github.com/sv/kdbgo"
)

// Global variables
var (
	runner *query.BenchmarkRunner
)

// Program option vars:
var (
	hostList []string
	// user     string
	// pass     string
	port int
)

func init() {
	log.Print("Init main")
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("hosts", "localhost", "Comma separated list of PostgreSQL hosts (pass multiple values for sharding reads on a multi-node setup)")
	pflag.String("port", "5010", "Which port to connect to on the database host")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	hosts := viper.GetString("hosts")
	port = viper.GetInt("port")

	runner = query.NewBenchmarkRunner(config)

	// Parse comma separated string of hosts and put in a slice (for multi-node setups)
	hostList = append(hostList, strings.Split(hosts, ",")...)
}

func main() {
	log.Print("Running main")
	runner.Run(&query.KDBPool, newProcessor)
}

type queryExecutorOptions struct {
	debug         bool
	printResponse bool
}

type processor struct {
	qProc *kdb.KDBConn
	opts  *queryExecutorOptions
}

func newProcessor() query.Processor {
	log.Print("Getting new processor")
	return &processor{}
}

func (p *processor) Init(workerNumber int) {
	//TODO Handle multiple hosts
	// log.Print("Init processor")
	con, err := kdb.DialKDB("localhost", port, "")
	p.qProc = con

	if err != nil {
		panic(err)
	}

	p.opts = &queryExecutorOptions{
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}
	// log.Print("done init processor")
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	log.Print("Processing query")
	kdbq := q.(*query.KDB)

	start := time.Now()
	if p.opts.debug {
		log.Print(kdbq.Query)
	}

	result, err := p.qProc.Call("runQuery", &kdb.K{Type: 10, Attr: kdb.NONE, Data: kdbq.Query[1]})

	if err != nil {
		return nil, err
	}

	if p.opts.printResponse {
		jsonSource := make(map[string]interface{})
		jsonSource["query"] = kdbq.Query[1]

		switch resultCont := result.Data.(type) {
		case kdb.Dict:
			jsonSource["results"] = getRowsFromDict(resultCont)
		case kdb.Table:
			size := resultCont.Data[0].Len()
			resultEntry := make([]map[string]interface{}, size)
			for row := 0; row < size; row++ {
				resultEntry[row] = make(map[string]interface{})
			}
			addRowsFromTable(resultCont, resultEntry, size)
			jsonSource["results"] = resultEntry
		default:
			errorMsg := fmt.Sprintf("unsupported type: %T", resultCont)
			panic(errorMsg)
		}
		jsonStr, err := json.MarshalIndent(jsonSource, "", "    ")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(jsonStr))
	}

	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, err
}

func getItem(cont interface{}, index int) interface{} {
	switch cont := cont.(type) {
	case []time.Time:
		return cont[index]
	case []int16:
		return cont[index]
	case []int32:
		return cont[index]
	case []int64:
		return cont[index]
	case []float32:
		return cont[index]
	case []float64:
		return cont[index]
	case []string:
		return cont[index]
	}
	errorMsg := fmt.Sprintf("unsupported type: %T", cont)
	panic(errorMsg)
}

func addRowsFromTable(source kdb.Table, target []map[string]interface{}, size int) {
	for row := 0; row < size; row++ {
		for i := 0; i < len(source.Columns); i++ {
			target[row][source.Columns[i]] = getItem(source.Data[i].Data, row)
		}
	}
}

func getRowsFromDict(source kdb.Dict) []map[string]interface{} {
	keys := source.Key.Data.(kdb.Table)
	values := source.Value.Data.(kdb.Table)
	size := keys.Data[0].Len()

	result := make([]map[string]interface{}, size)
	for row := 0; row < size; row++ {
		result[row] = make(map[string]interface{})
	}
	addRowsFromTable(keys, result, size)
	addRowsFromTable(values, result, size)

	return result
}
