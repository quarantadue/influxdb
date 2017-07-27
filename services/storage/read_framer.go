package storage

import (
	"io"
)

type frameEncoder struct {
	buf []byte
	ofs int
	c   int
}

func (e *frameEncoder) WriteSeries(v ReadResponse_SeriesFrame) error {
	sz := v.Size() + 1
	if e.ofs+sz > len(e.buf) {
		return io.EOF
	}
	e.buf[e.ofs] = byte(FrameTypeSeries)
	v.MarshalTo(e.buf[e.ofs+1:])
	e.ofs += sz
	return nil
}

func (e *frameEncoder) WriteIntegerPoints(v ReadResponse_PointsFrame) error {
	sz := v.Size() + 1
	if e.ofs+sz > len(e.buf) {
		return io.EOF
	}
	e.buf[e.ofs] = byte(FrameTypePoints)
	v.MarshalTo(e.buf[e.ofs+1:])
	e.ofs += sz
	return nil
}
