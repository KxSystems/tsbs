package kdb

import (
	"fmt"
	"strings"
	"time"

	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/questdb/tsbs/pkg/query"
)

const qFormat string = "2006.01.02D15:04:05.000000000"

// IoT produces Kdb-specific queries for all the iot query types.
type IoT struct {
	*iot.Core
	*BaseGenerator
}

// NewIoT makes an IoT object ready to generate Queries.
func NewIoT(start, end time.Time, scale int, g *BaseGenerator) *IoT {
	c, err := iot.NewCore(start, end, scale)
	databases.PanicIfErr(err)
	return &IoT{
		Core:          c,
		BaseGenerator: g,
	}
}

func (i *IoT) getTrucksWhereWithNames(names []string) string {
	nameClauses := []string{}
	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("`%s", s))
	}

	combinedTrucknameClause := strings.Join(nameClauses, "")
	return "((), " + combinedTrucknameClause + ")"
}

func (i *IoT) getTruckWhereString(nTrucks int) string {
	names, err := i.GetRandomTrucks(nTrucks)
	if err != nil {
		panic(err.Error())
	}
	return i.getTrucksWhereWithNames(names)
}

// LastLocByTruck finds the truck location for nTrucks.
func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	qsql := fmt.Sprintf("LastLocByTruck[%s]", i.getTruckWhereString(nTrucks))

	humanLabel := "kdb+ last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)

	i.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	qsql := fmt.Sprintf("LastLocPerTruck[`%s]", i.GetRandomFleet())

	humanLabel := "kdb+ last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	qsql := fmt.Sprintf("TrucksWithLowFuel[`%s]", i.GetRandomFleet())

	humanLabel := "kdb+ trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	qsql := fmt.Sprintf("TrucksWithHighLoad[`%s]", i.GetRandomFleet())

	humanLabel := "kdb+ trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
func (i *IoT) StationaryTrucks(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	start := interval.Start().Format(qFormat)
	end := interval.End().Format(qFormat)
	qsql := fmt.Sprintf("StationaryTrucks[%s;%s;`%s]", start, end, i.GetRandomFleet())

	humanLabel := "kdb+ stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.LongDrivingSessionDuration)
	start := interval.Start().Format(qFormat)
	end := interval.End().Format(qFormat)
	qsql := fmt.Sprintf("TrucksWithLongDrivingSessions[%s;%s;`%s;%d]", start, end, i.GetRandomFleet(), tenMinutePeriods(35, iot.LongDrivingSessionDuration))

	humanLabel := "kdb+ trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.DailyDrivingDuration)
	start := interval.Start().Format(qFormat)
	end := interval.End().Format(qFormat)
	qsql := fmt.Sprintf("TrucksWithLongDailySessions (%s;%s;%s;%d)", start, end, i.GetRandomFleet(), tenMinutePeriods(35, iot.DailyDrivingDuration))

	humanLabel := "kdb+ trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: drove more than 10 hours in the last 24 hours", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	qsql := "AvgVsProjectedFuelConsumption[]"

	humanLabel := "kdb+ average vs projected fuel consumption per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	qsql := "AvgDailyDrivingDuration[]"

	humanLabel := "kdb+ average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {
	qsql := "AvgDailyDrivingSession[]"

	humanLabel := "kdb+ average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	qsql := "AvgLoad[]"

	humanLabel := "kdb+ average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
func (i *IoT) DailyTruckActivity(qi query.Query) {
	qsql := "DailyTruckActivity[]"

	humanLabel := "kdb+ daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	qsql := "TruckBreakdownFrequency[]"

	humanLabel := "kdb+ truck breakdown frequency per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, qsql)
}

// tenMinutePeriods calculates the number of 10 minute periods that can fit in
// the time duration if we subtract the minutes specified by minutesPerHour value.
// E.g.: 4 hours - 5 minutes per hour = 3 hours and 40 minutes = 22 ten minute periods
func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}
