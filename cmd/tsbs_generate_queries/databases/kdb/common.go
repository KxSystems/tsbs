package kdb

import (
	"log"
	"time"

	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/questdb/tsbs/pkg/query"
)

// BaseGenerator contains settings specific for KDB database.
type BaseGenerator struct {
}

// GenerateEmptyQuery returns an empty query.KDB.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewKDB()
}

// fillInQuery fills the query struct with data.
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, qsql string) {
	log.Print("Filling in Query")
	q := qi.(*query.KDB)

	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.Query = []string{"query", qsql}
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}

// NewIoT creates a new iot use case query generator.
func (g *BaseGenerator) NewIoT(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := iot.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	iot := &IoT{
		BaseGenerator: g,
		Core:          core,
	}

	return iot, nil
}
