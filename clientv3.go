package getcdv3

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	RETRY_C = 2
)

// <summary>
// Clientv3
// <summary>
type Clientv3 struct {
	cli    *clientv3.Client
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *Clientv3) Grant(ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return s.cli.Grant(s.ctx, ttl)
}

func (s *Clientv3) GrantCtx(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return s.cli.Grant(ctx, ttl)
}

func (s *Clientv3) KeepAlive(id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return s.cli.KeepAlive(s.ctx, id)
}

func (s *Clientv3) KeepAliveCtx(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return s.cli.KeepAlive(ctx, id)
}

func (s *Clientv3) Cancel() {
	s.cancel()
}

func (s *Clientv3) Delete(key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return s.cli.Delete(s.ctx, key)
}

func (s *Clientv3) DeleteCtx(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return s.cli.Delete(ctx, key)
}

func (s *Clientv3) Get(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return s.cli.Get(s.ctx, key, opts...)
}

func (s *Clientv3) GetCtx(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return s.cli.Get(ctx, key, opts...)
}

func (s *Clientv3) Put(key string, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return s.cli.Put(s.ctx, key, val, opts...)
}

func (s *Clientv3) PutCtx(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return s.cli.Put(ctx, key, val, opts...)
}

func (s *Clientv3) Watch(key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return s.cli.Watch(s.ctx, key, opts...)
}

func (s *Clientv3) WatchCtx(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return s.cli.Watch(ctx, key, opts...)
}

func (s *Clientv3) Close() {
	s.cli.Close()
}
