package storage

import (
	"encoding/binary"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
	"golang.org/x/net/context"
)

//go:generate protoc -I$GOPATH/src -I. --gogofaster_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,plugins=grpc:. storage.proto

type rpcService struct {
	TSDBStore *tsdb.Store

	MetaClient interface {
		Database(name string) *meta.DatabaseInfo
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}

	Logger zap.Logger
}

func (r *rpcService) Capabilities(context.Context, *types.Empty) (*CapabilitiesResponse, error) {
	panic("implement me")
}

func (r *rpcService) Hints(context.Context, *types.Empty) (*HintsResponse, error) {
	panic("implement me")
}

func (r *rpcService) Read(req *ReadRequest, stream Storage_ReadServer) error {

	const BatchSize = 5000
	const FrameSize = 100 << 10 // 100 kb

	return nil
}

type integerPoints struct {
	c   uint32
	buf []byte
	d   []byte
}

func newIntegerPoints(sz int) *integerPoints {
	i := &integerPoints{buf: make([]byte, sz*16+4)}
	i.Reset()
	return i
}

func (i *integerPoints) Write(t, v int64) uint32 {
	binary.BigEndian.PutUint64(i.d, uint64(t))
	binary.BigEndian.PutUint64(i.d[8:], uint64(v))
	i.d = i.d[16:]
	i.c++
	return i.c
}

func (i *integerPoints) Buf() []byte {
	if i.c == 0 {
		return nil
	}

	binary.BigEndian.PutUint32(i.buf[:4], i.c)
	return i.buf[:i.c*16+4]
}

func (i *integerPoints) Reset() {
	i.c = 0
	i.d = i.buf[4:]
}
