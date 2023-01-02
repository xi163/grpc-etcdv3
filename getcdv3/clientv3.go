package getcdv3

import (
	"context"

	"github.com/cwloo/gonet/logs"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// <summary>
// Clientv3
// <summary>
type Clientv3 interface {
	Ctx() context.Context
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Put(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
	Cancel()
	Free()
	Close()
}

// <summary>
// clientv3_
// <summary>
type clientv3_ struct {
	cli    *clientv3.Client
	ctx    context.Context
	cancel context.CancelFunc
}

func newClientv3(c *clientv3.Client) Clientv3 {
	// ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	return &clientv3_{
		cli:    c,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *clientv3_) assert() {
	switch s.cli {
	case nil:
		logs.Fatalf("error")
	}
}

func (s *clientv3_) Ctx() context.Context {
	return s.ctx
}

func (s *clientv3_) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	s.assert()
	return s.cli.Grant(ctx, ttl)
}

func (s *clientv3_) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	s.assert()
	return s.cli.KeepAlive(ctx, id)
}

func (s *clientv3_) Cancel() {
	s.cancel()
}

func (s *clientv3_) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	s.assert()
	return s.cli.Delete(ctx, key)
}

func (s *clientv3_) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	s.assert()
	return s.cli.Get(ctx, key, opts...)
}

func (s *clientv3_) Put(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	s.assert()
	return s.cli.Put(ctx, key, val, opts...)
}

func (s *clientv3_) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	s.assert()
	return s.cli.Watch(ctx, key, opts...)
}

func (s *clientv3_) Free() {
	switch s.cli {
	case nil:
	default:
		etcds.Put(s)
	}
}

func (s *clientv3_) Close() {
	switch s.cli {
	case nil:
	default:
		_ = s.cli.Close()
		s.cli = nil
	}
}
