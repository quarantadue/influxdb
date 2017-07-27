// +build off

package query

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"bytes"
	"encoding/binary"
	"errors"

	"github.com/gogo/protobuf/codec"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/yamux"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/storage"
	context2 "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Command represents the program execution for "influx_inspect export".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	addr            string
	cpuProfile      string
	memProfile      string
	database        string
	retentionPolicy string
	startTime       int64
	endTime         int64
	limit           int
	offset          int
	desc            bool
	silent          bool
	proto           string
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

func parseTime(v string) (int64, error) {
	if s, err := time.Parse(time.RFC3339, v); err == nil {
		return s.UnixNano(), nil
	}

	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i, nil
	}

	return 0, errors.New("invalid time")
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	var start, end string
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	fs.StringVar(&cmd.cpuProfile, "cpuprofile", "", "CPU profile name")
	fs.StringVar(&cmd.memProfile, "memprofile", "", "memory profile name")
	fs.StringVar(&cmd.addr, "addr", ":8082", "the RPC address")
	fs.StringVar(&cmd.database, "database", "", "Optional: the database to export")
	fs.StringVar(&cmd.retentionPolicy, "retention", "", "Optional: the retention policy to export (requires -database)")
	fs.StringVar(&start, "start", "", "Optional: the start time to export (RFC3339 format)")
	fs.StringVar(&end, "end", "", "Optional: the end time to export (RFC3339 format)")
	fs.IntVar(&cmd.limit, "limit", 10, "Optional: limit number of rows")
	fs.IntVar(&cmd.offset, "offset", 0, "Optional: start offset for rows")
	fs.BoolVar(&cmd.desc, "desc", false, "Optional: return results in descending order")
	fs.BoolVar(&cmd.silent, "silent", false, "silence output")
	fs.StringVar(&cmd.proto, "proto", "yamux", "yamux, grpc or tcp")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = func() {
		fmt.Fprintln(cmd.Stdout, "Query via RPC")
		fmt.Fprintf(cmd.Stdout, "Usage: %s query [flags]\n\n", filepath.Base(os.Args[0]))
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}

	// set defaults
	if start != "" {
		if t, err := parseTime(start); err != nil {
			return err
		} else {
			cmd.startTime = t
		}
	} else {
		cmd.startTime = models.MinNanoTime
	}
	if end != "" {
		if t, err := parseTime(end); err != nil {
			return err
		} else {
			cmd.endTime = t
		}
	} else {
		// set end time to max if it is not set.
		cmd.endTime = models.MaxNanoTime
	}

	if err := cmd.validate(); err != nil {
		return err
	}

	switch cmd.proto {
	case "yamux", "tcp":
		if cmd.addr == ":8082" {
			switch cmd.proto {
			case "yamux":
				cmd.addr = ":8083"
			default:
				cmd.addr = ":8084"
			}
		}
		return cmd.queryTRPC()

	default:
		return cmd.queryGRPC()
	}
}

func (cmd *Command) validate() error {
	if cmd.retentionPolicy != "" && cmd.database == "" {
		return fmt.Errorf("must specify a db")
	}
	if cmd.startTime != 0 && cmd.endTime != 0 && cmd.endTime < cmd.startTime {
		return fmt.Errorf("end time before start time")
	}
	return nil
}

func (cmd *Command) query(c storage.StorageClient) error {
	var req storage.ReadRequest
	req.Database = cmd.database
	req.Limit = int64(cmd.limit)
	req.Ascending = true
	req.TimestampRange.Start = cmd.startTime
	req.TimestampRange.End = cmd.endTime

	stream, err := c.Read(context.Background(), &req)
	if err != nil {
		return err
	}

	sum := int64(0)
	var buf [1024]byte
	var line []byte
	wr := bufio.NewWriter(os.Stdout)

	now := time.Now()
	defer func() {
		dur := time.Since(now)
		fmt.Printf("time: %v\n", dur)
	}()

	var dec storage.IntegerPointsDecoder

	for {
		var rep storage.ReadResponse

		if err := stream.RecvMsg(&rep); err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		dec.SetBuf(rep.GetPoints())

		if cmd.silent {
			for dec.Next() {
				_, v := dec.Read()
				sum += v
			}
		} else {
			for dec.Next() {
				t, v := dec.Read()

				line = buf[:0]
				wr.Write(strconv.AppendInt(line, t, 10))
				wr.WriteByte(' ')

				line = buf[:0]
				wr.Write(strconv.AppendInt(line, v, 10))
				wr.WriteString("\n")
				wr.Flush()

				sum += v
			}
		}
	}

	fmt.Println("sum", sum)

	return nil
}

type dialler func() (io.ReadWriteCloser, error)

func tcpDialler(addr string) dialler {
	return func() (io.ReadWriteCloser, error) {
		return net.Dial("tcp", addr)
	}
}

func yamuxDialler(dial dialler) dialler {
	var cn io.ReadWriteCloser
	var err error
	errorDial := func() (io.ReadWriteCloser, error) {
		return nil, err
	}

	cn, err = dial()
	if err != nil {
		return errorDial
	}

	cfg := yamux.DefaultConfig()
	cfg.MaxStreamWindowSize = 50 << 20

	session, err := yamux.Client(cn, cfg)
	if err != nil {
		return errorDial
	}

	return func() (io.ReadWriteCloser, error) {
		return session.OpenStream()
	}
}

func getDialler(addr string, mux string) dialler {
	dial := tcpDialler(addr)
	switch mux {
	case "yamux":
		dial = yamuxDialler(dial)
	}

	return dial
}

func (cmd *Command) queryTRPC() error {
	dial := getDialler(cmd.addr, cmd.proto)

	conn, err := dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	c := &tcpStorageClient{conn}
	return cmd.query(c)
}

func (cmd *Command) queryGRPC() error {
	grpc.EnableTracing = false
	opts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithCodec(codec.New(1000)), grpc.WithInitialConnWindowSize(2 ^ 18), grpc.WithInitialWindowSize(2 ^ 18)}
	conn, err := grpc.Dial(cmd.addr, opts...)
	if err != nil {
		return err
	}
	defer conn.Close()

	c := storage.NewStorageClient(conn)
	return cmd.query(c)
}

type tcpStorageClient struct {
	cn io.ReadWriteCloser
}

func (c *tcpStorageClient) Read(ctx context.Context, in *storage.ReadRequest, opts ...grpc.CallOption) (storage.Storage_ReadClient, error) {
	sz := in.Size()

	buf := make([]byte, sz+4)

	n, err := in.MarshalTo(buf[4:])
	if err != nil {
		return nil, err
	}

	binary.BigEndian.PutUint32(buf[:4], uint32(n))

	d := bytes.NewBuffer(buf[:n+4])
	_, err = io.Copy(c.cn, d)
	if err != nil {
		return nil, err
	}

	return &tcpStorageReadClient{cn: c.cn}, nil
}

type tcpStorageReadClient struct {
	cn  io.ReadWriteCloser
	buf []byte
	grpc.ClientStream
}

func (tcpStorageReadClient) Recv() (*storage.ReadResponse, error) {
	return nil, io.EOF
}

func (tcpStorageReadClient) Header() (metadata.MD, error) {
	panic("implement me")
}

func (tcpStorageReadClient) Trailer() metadata.MD {
	panic("implement me")
}

func (tcpStorageReadClient) CloseSend() error {
	panic("implement me")
}

func (tcpStorageReadClient) Context() context2.Context {
	panic("implement me")
}

func (tcpStorageReadClient) SendMsg(m interface{}) error {
	panic("implement me")
}

func (c *tcpStorageReadClient) RecvMsg(m interface{}) error {
	r, ok := m.(proto.Unmarshaler)
	if !ok {
		return fmt.Errorf("not a protobuf")
	}

	var buf [4]byte
	_, err := io.ReadAtLeast(c.cn, buf[:], 4)
	if err != nil {
		return err
	}

	sz := binary.BigEndian.Uint32(buf[:])
	if sz == 0 {
		return io.EOF
	}

	if cap(c.buf) < int(sz) {
		c.buf = make([]byte, sz)
	}
	c.buf = c.buf[:sz]

	_, err = io.ReadAtLeast(c.cn, c.buf, int(sz))
	if err != nil {
		return err
	}

	return r.Unmarshal(c.buf)
}
