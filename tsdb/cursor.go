package tsdb

import "github.com/influxdata/influxdb/influxql"

// EOF represents a "not found" key returned by a Cursor.
const EOF = influxql.ZeroTime

// Cursor represents an iterator over a series.
type Cursor interface {
	// TODO(sgc): implement
	// Close()
	SeriesKey() string
}

type IntegerCursor interface {
	Cursor
	Next() (key int64, value int64)
}

type CursorRequest struct {
	Measurement string
	Series      string
	Field       string
	Ascending   bool
	StartTime   int64
	EndTime     int64
}
