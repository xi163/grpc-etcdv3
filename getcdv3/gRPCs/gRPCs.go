package gRPCs

import (
	"github.com/cwloo/gonet/core/base/sys"
	"github.com/cwloo/gonet/logs"
	"google.golang.org/grpc"
)

type Dial func(unique bool, schema, node, host string) (*grpc.ClientConn, error)

// <summary>
// RPCs
// <summary>
type RPCs interface {
	Schema() string
	Node() string
	Host() string
	Get() (conn ClientConn, e error)
	Put(conn ClientConn)
	Close(reset func(ClientConn))
}

type rpcs struct {
	unique bool
	schema string
	node   string
	host   string
	dial   Dial
	pool   sys.FreeValues
}

func newRPCs(unique bool, schema, node, host string, dial Dial) RPCs {
	s := &rpcs{
		unique: unique,
		schema: schema,
		node:   node,
		host:   host,
		dial:   dial,
	}
	s.pool = *sys.NewFreeValuesWith(s.new)
	s.assert()
	return s
}

func (s *rpcs) assert() {
	switch s.schema {
	case "":
		logs.Fatalf("error")
	}
	switch s.node {
	case "":
		logs.Fatalf("error")
	}
	switch s.host {
	case "":
		logs.Fatalf("error")
	}
	switch s.dial {
	case nil:
		logs.Fatalf("error")
	}
}

func (s *rpcs) Schema() string {
	return s.schema
}

func (s *rpcs) Node() string {
	return s.node
}

func (s *rpcs) Host() string {
	return s.host
}

func (s *rpcs) new(cb func(error, ...any)) (conn any, e error) {
	c, err := s.dial(s.unique, s.schema, s.node, s.host)
	e = err
	switch err {
	case nil:
		conn = newClientConn(s, c)
		cb(err, s.schema, s.node, s.host)
	default:
		cb(err, s.schema, s.node, s.host)
	}
	return
}

func (s *rpcs) Get() (conn ClientConn, e error) {
	v, err := s.pool.Get()
	e = err
	switch err {
	case nil:
		conn = v.(ClientConn)
	default:
		logs.Errorf(err.Error())
	}
	return
}

func (s *rpcs) Put(conn ClientConn) {
	switch conn.RPCs() == s {
	case true:
		s.pool.Put(conn)
	default:
		logs.Fatalf("error")
	}
}

func (s *rpcs) Close(reset func(ClientConn)) {
	s.pool.Reset(func(value any) {
		reset(value.(ClientConn))
		value.(ClientConn).Close()
	}, false)
}
