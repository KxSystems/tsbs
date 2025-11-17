package kdb

import (
	"io"

	"github.com/questdb/tsbs/pkg/data"
	"github.com/questdb/tsbs/pkg/data/serialize"
)

const qFormat string = "2006.01.02D15:04:05.000000000"

// Serializer writes a Point in a serialized form for KDB
type Serializer struct{}

// Serialize writes Point p to the given Writer w, so it can be
// loaded by the KDB loader. The format is CSV.
func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	//This function is run row-by-row. p is a single row

	buf := make([]byte, 0, 256)
	// Append table name first
	buf = serialize.FastFormatAppend(p.MeasurementName(), buf)
	buf = append(buf, ',')
	ts := []byte(p.Timestamp().UTC().Format(qFormat))
	buf = append(buf, ts...)

	//Append tags
	for _, v := range p.TagValues() {
		buf = append(buf, ',')
		buf = serialize.FastFormatAppend(v, buf)
	}
	//Append values
	for _, v := range p.FieldValues() {
		buf = append(buf, ',')
		buf = serialize.FastFormatAppend(v, buf)
	}

	buf = append(buf, '\n')
	_, err := w.Write(buf)
	return err
}
