package main

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/questdb/tsbs/pkg/data"
	"github.com/questdb/tsbs/pkg/data/usecases/common"
	"github.com/questdb/tsbs/pkg/targets"
	kdb "github.com/sv/kdbgo"
)

type fileDataSource struct {
	scanner *bufio.Scanner
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}
	return data.NewLoadedPoint(d.scanner.Bytes())
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders { return nil }

type batch struct {
	data        [][]byte
	metricCount uint64
}

func (b *batch) Len() uint {
	return uint(len(b.data))
}

func (b *batch) Append(item data.LoadedPoint) {
	row := item.Data.([]byte)
	temp := make([]byte, len(row))
	copy(temp, row)
	b.data = append(b.data, temp)
	b.metricCount += uint64(bytes.Count(row, []byte(",")))
}

// /////////////////////////////////
// transforming data to Q binary //
// /////////////////////////////////
func toQ(data string, qtype byte) *kdb.K {
	switch qtype {
	case 'S':
		return kdb.Symbol(data)
	case 'I':
		x, _ := strconv.ParseInt(data, 0, 32)
		return kdb.Int(int32(x))
	case 'F':
		x, _ := strconv.ParseFloat(data, 64)
		return kdb.Float(x)
	case 'J':
		x, _ := strconv.ParseInt(data, 0, 64)
		return kdb.Long(x)
	case 'P':
		x, _ := strconv.ParseInt(data, 0, 64)
		return &kdb.K{Type: -kdb.KP, Attr: kdb.NONE, Data: time.Unix(x, 0).UTC()}
	default:
		panic("unsupported type" + string(qtype))
	}
}

func toQList(data []string, schema string) *kdb.K {
	var temp = make([]*kdb.K, len(data))
	for ind, val := range data {
		temp[ind] = toQ(val, schema[ind])
	}

	return kdb.NewList(temp...)
}

func influxToQ(value lineprotocol.Value, qtype byte) *kdb.K {
	switch qtype {
	case 'S':
		return kdb.Symbol(value.StringV())
	case 'I':
		return kdb.Int(int32(value.IntV()))
	case 'F':
		return kdb.Float(value.FloatV())
	case 'J':
		return kdb.Long(value.IntV())
	case 'P':
		return &kdb.K{Type: -kdb.KP, Attr: kdb.NONE, Data: time.Unix(value.IntV(), 0).UTC()}
	default:
		panic("unsupported type" + string(qtype))
	}
}

func influxToQList(decoder *lineprotocol.Decoder, measurement string, addMeasurement bool) *kdb.K {
	var schema string
	var temp []*kdb.K
	index := 0
	if addMeasurement {
		schema = schemas[measurement]
		temp = make([]*kdb.K, len(schema))
		temp[0] = kdb.Symbol(string(measurement))
		index++
	} else {
		schema = schemas[measurement][1:]
		temp = make([]*kdb.K, len(schema))
	}
	// leave timestamp empty, first tags and values must be read
	index++

	for {
		key, val, err := decoder.NextTag()
		if err != nil {
			panic(err)
		}
		if key == nil {
			break
		}
		temp[index] = toQ(string(val), schema[index])
		index++
	}
	for {
		key, val, err := decoder.NextField()
		if err != nil {
			panic(err)
		}
		if key == nil {
			break
		}
		temp[index] = influxToQ(val, schema[index])
		index++
	}
	t, _ := decoder.Time(lineprotocol.Nanosecond, time.Time{})
	println(t.String())
	if addMeasurement {
		index = 1
	} else {
		index = 0
	}
	temp[index] = &kdb.K{Type: -kdb.KP, Attr: kdb.NONE, Data: t}

	return kdb.NewList(temp...)
}

var schemas = map[string]string{
	"readings":    "SPSSSSSFFFFFFFFFF",
	"diagnostics": "SPSSSSSFFFIII",
	"cpu":         "SPSSSISSSIISIIIIIIIIII",
	"disk":        "SPSSSISSSIISSSJJJIJJJ",
	"diskio":      "SPSSSISSSIISSIIIIIII",
	"kernel":      "SPSSSISSSIISIIIIII",
	"mem":         "SPSSSISSSIISJJJJJJFFF",
	"net":         "SPSSSISSSIISSIIIIIIII",
	"nginx":       "SPSSSISSSIISJSIIIIIII",
	"postgresl":   "SPSSSISSSIISIIIIIIIIIIIIJIII",
	"redis":       "SPSSSISSSIISISIIIIIIIIIIJJJJIIIIIIIIIIIIIIIII",
}

type batchBinary struct {
	qData       []*kdb.K
	metricCount uint
	useInflux   bool
}

func (b *batchBinary) Len() uint {
	return uint(len(b.qData))
}

func (b *batchBinary) Append(item data.LoadedPoint) {
	var qList *kdb.K
	if b.useInflux {
		dec := lineprotocol.NewDecoderWithBytes(item.Data.([]byte))
		dec.Next()
		measurement, _ := dec.Measurement()
		qList = influxToQList(dec, string(measurement), true)
	} else {
		row := strings.Split(string(item.Data.([]byte)), ",")
		qList = toQList(row, schemas[row[0]])
	}
	b.qData = append(b.qData, qList)
	b.metricCount += uint(qList.Len() - 1)
}

type batchBinaryDict struct {
	qData       map[string][]*kdb.K
	rowCount    uint
	metricCount uint
	useInflux   bool
}

func (b *batchBinaryDict) Len() uint {
	return b.rowCount
}

func (b *batchBinaryDict) Append(item data.LoadedPoint) {
	var qList *kdb.K
	var tablename string
	if b.useInflux {
		dec := lineprotocol.NewDecoderWithBytes(item.Data.([]byte))
		dec.Next()
		measurement, _ := dec.Measurement()
		tablename = string(measurement)
		qList = influxToQList(dec, tablename, false)
	} else {
		row := strings.Split(string(item.Data.([]byte)), ",")
		tablename = row[0]
		qList = toQList(row[1:], schemas[tablename][1:])
	}
	_, ok := b.qData[tablename]
	if !ok {
		b.qData[tablename] = make([]*kdb.K, 0)
	}
	b.qData[tablename] = append(b.qData[tablename], qList)
	b.rowCount++
	b.metricCount += uint(qList.Len())
}

type factory struct {
	dataFormat string
	useInflux  bool
}

func (f *factory) New() targets.Batch {
	switch f.dataFormat {
	case "string":
		return &batch{data: make([][]byte, 0)}
	case "binary":
		return &batchBinary{qData: make([]*kdb.K, 0), useInflux: f.useInflux}
	case "binaryDict":
		return &batchBinaryDict{qData: make(map[string][]*kdb.K), useInflux: f.useInflux}
	default:
		errorMsg := fmt.Sprintf("Unsupported data format: %s", f.dataFormat)
		panic(errorMsg)
	}
}
