package storage

import (
	"net"

	"time"

	"github.com/gogo/protobuf/codec"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
	"google.golang.org/grpc"
)

type grpcServer struct {
	addr string
	ln   net.Listener
	rpc  *grpc.Server

	TSDBStore  *tsdb.Store
	MetaClient interface {
		Database(name string) *meta.DatabaseInfo
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}
	Logger zap.Logger
}

func newGRPCServer(addr string) *grpcServer {
	return &grpcServer{addr: addr}
}

func (s *grpcServer) Open() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.ln = listener
	go s.serve()
	return nil
}

func (s *grpcServer) Close() error {
	s.rpc.Stop()
	return nil
}

func (s *grpcServer) serve() {
	grpc.EnableTracing = false
	s.rpc = grpc.NewServer(grpc.CustomCodec(codec.New(1000)), grpc.InitialConnWindowSize(2^18), grpc.InitialWindowSize(2^18))
	RegisterStorageServer(s.rpc, &rpcService{TSDBStore: s.TSDBStore, MetaClient: s.MetaClient, Logger: s.Logger})
	s.Logger.Info("grpc listening", zap.String("address", s.ln.Addr().String()))
	s.rpc.Serve(s.ln)
}
