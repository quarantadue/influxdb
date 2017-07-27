package storage

import (
	"io"
	"log"
	"net"

	"bytes"
	"encoding/binary"

	"github.com/hashicorp/yamux"
	"github.com/uber-go/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type tcpServer struct {
	addr    string
	ln      net.Listener
	handler func(*tcpServer, io.ReadWriteCloser)
	ch      chan struct{}
	mux     string

	RPC    *rpcService
	Logger zap.Logger
}

func newTCPServer(addr string, mux string) *tcpServer {
	s := &tcpServer{addr: addr, ch: make(chan struct{}), mux: mux}

	switch mux {
	case "yamux":
		s.handler = (*tcpServer).yamuxHandler

	default:
		s.handler = (*tcpServer).streamHandler
	}

	return s
}

func (s *tcpServer) Open() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = l

	s.Logger.Info("TCP RPC listening", zap.String("mux", s.mux), zap.String("address", s.ln.Addr().String()))

	go s.serve()
	return nil
}

func (s *tcpServer) Close() error {
	return s.ln.Close()
}

func (s *tcpServer) serve() {
	for {
		cn, err := s.ln.Accept()
		if err != nil {
			// no retry right now
			return
		}

		go s.handler(s, cn)
	}
}

func (s *tcpServer) yamuxHandler(cn io.ReadWriteCloser) {
	defer func() {
		cn.Close()
	}()

	cfg := yamux.DefaultConfig()
	cfg.MaxStreamWindowSize = 50 << 20

	session, err := yamux.Server(cn, cfg)
	if err != nil {
		log.Fatal("muxHandler: smux.Server failed", err)
	}

	for {
		stream, err := session.AcceptStream()
		if err != nil {
			return
		}

		go s.streamHandler(stream)
	}
}

func (s *tcpServer) streamHandler(cn io.ReadWriteCloser) {
	defer func() {
		cn.Close()
	}()

	var buf [4]byte

	_, err := io.ReadAtLeast(cn, buf[:], 4)
	if err != nil {
		return
	}

	sz := binary.BigEndian.Uint32(buf[:])
	s.Logger.Info("request", zap.String("mux", s.mux), zap.String("address", s.ln.Addr().String()))

	d := make([]byte, sz)
	_, err = io.ReadAtLeast(cn, d, int(sz))
	if err != nil {
		return
	}

	var req ReadRequest
	err = req.Unmarshal(d)
	if err != nil {
		return
	}

	tcp := &tcpReadServer{wr: cn}
	s.RPC.Read(&req, tcp)
}

type tcpReadServer struct {
	wr  io.Writer
	buf []byte
	grpc.ServerStream
}

func (s *tcpReadServer) Send(r *ReadResponse) error {
	sz := r.Size() + 4
	if sz > cap(s.buf) {
		s.buf = make([]byte, sz)
	}

	s.buf = s.buf[:sz]
	n, err := r.MarshalTo(s.buf[4:])
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint32(s.buf[:4], uint32(n))

	b := bytes.NewBuffer(s.buf[:n+4])
	_ = b
	_, err = io.Copy(s.wr, b)
	return err
}

func (s *tcpReadServer) SetHeader(metadata.MD) error {
	panic("implement me")
}

func (s *tcpReadServer) SendHeader(metadata.MD) error {
	panic("implement me")
}

func (s *tcpReadServer) SetTrailer(metadata.MD) {
	panic("implement me")
}

func (s *tcpReadServer) Context() context.Context {
	panic("implement me")
}

func (s *tcpReadServer) SendMsg(m interface{}) error {
	panic("implement me")
}

func (s *tcpReadServer) RecvMsg(m interface{}) error {
	panic("implement me")
}
