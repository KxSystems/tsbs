package kdb

import (
	"fmt"
	"strings"
	"time"

	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/questdb/tsbs/pkg/query"
)

// Devops produces KDB-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("`%s", s))
	}

	combinedHostnameClause := strings.Join(hostnameClauses, "")
	return "((), " + combinedHostnameClause + ")"
}

func (d *Devops) getHostWhereString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	databases.PanicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	start := interval.Start().Format(qFormat)
	end := interval.End().Format(qFormat)

	qsql := fmt.Sprintf("single_groupby_%d_by_minute[%s;%s;%s]", numMetrics, d.getHostWhereString(nHosts), start, end)

	humanLabel := fmt.Sprintf("KDB %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// GroupByOrderByLimit benchmarks a query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	end := interval.End().Format(qFormat)

	qsql := fmt.Sprintf("groupby_orderby_limit[%s]", end)

	humanLabel := "KDB max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())

	d.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	start := interval.Start().Format(qFormat)
	end := interval.End().Format(qFormat)

	var qsql string
	if numMetrics == 10 {
		qsql = fmt.Sprintf("double_groupby_all[%s;%s]", start, end)
	} else {
		qsql = fmt.Sprintf("double_groupby_%d[%s;%s]", numMetrics, start, end)
	}

	humanLabel := devops.GetDoubleGroupByLabel("KDB", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)
	start := interval.Start().Format(qFormat)
	end := interval.End().Format(qFormat)

	qsql := fmt.Sprintf("cpu_max_by_hour[%s;%s;%s]", d.getHostWhereString(nHosts), start, end)

	humanLabel := devops.GetMaxAllLabel("KDB", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())

	d.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	humanLabel := "KDB last row per host"
	humanDesc := humanLabel + ": cpu"
	qsql := "lastpoint[]"
	d.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts)
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	start := interval.Start().Format(qFormat)
	end := interval.End().Format(qFormat)

	var qsql string
	if nHosts == 0 {
		qsql = fmt.Sprintf("high_cpu_all[%s;%s]", start, end)
	} else {
		qsql = fmt.Sprintf("high_cpu[%s;%s;%s]", d.getHostWhereString(nHosts), start, end)
	}

	humanLabel := fmt.Sprintf("KDB get high cpu for %d hosts", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())

	d.fillInQuery(qi, humanLabel, humanDesc, qsql)
}
