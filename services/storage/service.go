package storage

import (
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
)

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	addr  string
	addr2 string
	addr3 string

	grpc  *grpcServer
	trpcy *tcpServer // yamux
	trpcd *tcpServer // direct

	TSDBStore  *tsdb.Store
	MetaClient interface {
		Database(name string) *meta.DatabaseInfo
		ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	}
	Logger zap.Logger
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	s := &Service{
		addr:   c.BindAddress,
		addr2:  c.BindAddress2,
		addr3:  c.BindAddress3,
		Logger: zap.New(zap.NullEncoder()),
	}

	return s
}

// Open starts the service.
func (s *Service) Open() error {
	s.Logger.Info("Starting storage service")

	grpc := newGRPCServer(s.addr)
	grpc.Logger = s.Logger
	grpc.TSDBStore = s.TSDBStore
	grpc.MetaClient = s.MetaClient
	if err := grpc.Open(); err != nil {
		return err
	}

	s.grpc = grpc

	rs := &rpcService{TSDBStore: s.TSDBStore, MetaClient: s.MetaClient, Logger: s.Logger}

	trpc := newTCPServer(s.addr2, "yamux")
	trpc.RPC = rs
	trpc.Logger = s.Logger
	if err := trpc.Open(); err != nil {
		s.grpc.Close()
		return err
	}
	s.trpcy = trpc

	trpc = newTCPServer(s.addr3, "tcp")
	trpc.RPC = rs
	trpc.Logger = s.Logger
	if err := trpc.Open(); err != nil {
		s.grpc.Close()
		s.trpcy.Close()
		return err
	}
	s.trpcd = trpc

	return nil
}

func (s *Service) Close() error {
	if s.grpc != nil {
		s.grpc.Close()
	}

	if s.trpcy != nil {
		s.trpcy.Close()
	}

	if s.trpcd != nil {
		s.trpcd.Close()
	}

	return nil
}

// WithLogger sets the logger for the service.
func (s *Service) WithLogger(log zap.Logger) {
	s.Logger = log.With(zap.String("service", "storage"))
}
