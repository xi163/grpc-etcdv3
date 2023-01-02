package getcdv3

import (
	"context"
	"sync"

	"github.com/cwloo/gonet/logs"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	cli = newClient()
)

// <summary>
// Client
// <summary>
type Client interface {
	Cli() (Clientv3, error)
	Grant(ctx context.Context, ttl int64) (resp *clientv3.LeaseGrantResponse, e error)
	GrantRelease(ctx context.Context, ttl int64) (resp *clientv3.LeaseGrantResponse, e error)
	KeepAlive(ctx context.Context, id clientv3.LeaseID) (ch <-chan *clientv3.LeaseKeepAliveResponse, e error)
	KeepAliveRelease(ctx context.Context, id clientv3.LeaseID) (ch <-chan *clientv3.LeaseKeepAliveResponse, e error)
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, e error)
	DeleteRelease(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, e error)
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.GetResponse, e error)
	GetRelease(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.GetResponse, e error)
	Put(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, e error)
	PutRelease(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, e error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) (c clientv3.WatchChan)
	WatchRelease(ctx context.Context, key string, opts ...clientv3.OpOption) (c clientv3.WatchChan)
	Cancel()
	Free()
	Close()
}

// <summary>
// client
// <summary>
type client struct {
	cli Clientv3
	l   *sync.RWMutex
}

func newClient() Client {
	s := &client{l: &sync.RWMutex{}}
	return s
}

func (s *client) get_client() (cli Clientv3) {
	s.l.RLock()
	cli = s.cli
	s.l.RUnlock()
	return
}

func (s *client) new_client() (cli Clientv3, err error) {
	s.l.Lock()
	switch s.cli {
	case nil:
		cli, err = etcds.Get()
		switch err {
		case nil:
			s.cli = cli
		}
	default:
		cli = s.cli
	}
	s.l.Unlock()
	return
}

func (s *client) get_cli() (cli Clientv3, err error) {
	cli = s.get_client()
	switch cli {
	case nil:
		cli, err = s.new_client()
	default:
	}
	return
}

func (s *client) Cli() (Clientv3, error) {
	return s.get_cli()
}

func (s *client) Grant(ctx context.Context, ttl int64) (resp *clientv3.LeaseGrantResponse, e error) {
	return s.grant(false, ctx, ttl)
}

func (s *client) GrantRelease(ctx context.Context, ttl int64) (resp *clientv3.LeaseGrantResponse, e error) {
	return s.grant(true, ctx, ttl)
}

func (s *client) grant(free bool, ctx context.Context, ttl int64) (resp *clientv3.LeaseGrantResponse, e error) {
RETRY:
	cli, err := s.get_cli()
	e = err
	switch err {
	case nil:
		logs.Errorf("")
		resp, e = cli.Grant(ctx, ttl)
		switch e {
		case nil:
			switch free {
			case true:
				s.Free()
			}
		default:
			logs.Errorf(e.Error())
			s.Close()
			goto RETRY
		}
	default:
		logs.Errorf(err.Error())
	}
	return
}

func (s *client) KeepAlive(ctx context.Context, id clientv3.LeaseID) (ch <-chan *clientv3.LeaseKeepAliveResponse, e error) {
	return s.keepAlive(false, ctx, id)
}

func (s *client) KeepAliveRelease(ctx context.Context, id clientv3.LeaseID) (ch <-chan *clientv3.LeaseKeepAliveResponse, e error) {
	return s.keepAlive(true, ctx, id)
}

func (s *client) keepAlive(free bool, ctx context.Context, id clientv3.LeaseID) (ch <-chan *clientv3.LeaseKeepAliveResponse, e error) {
RETRY:
	cli, err := s.get_cli()
	e = err
	switch err {
	case nil:
		logs.Errorf("")
		ch, e = cli.KeepAlive(ctx, id)
		switch e {
		case nil:
			switch free {
			case true:
				s.Free()
			}
		default:
			logs.Errorf(e.Error())
			s.Close()
			goto RETRY
		}
	default:
		logs.Errorf(err.Error())
	}
	return
}

func (s *client) Cancel() {
	cli, err := s.get_cli()
	switch err {
	case nil:
		logs.Errorf("")
		cli.Cancel()
	}
}

func (s *client) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, e error) {
	return s.delete(false, ctx, key, opts...)
}

func (s *client) DeleteRelease(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, e error) {
	return s.delete(true, ctx, key, opts...)
}

func (s *client) delete(free bool, ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, e error) {
RETRY:
	cli, err := s.get_cli()
	e = err
	switch err {
	case nil:
		logs.Errorf("")
		resp, e = cli.Delete(ctx, key, opts...)
		switch e {
		case nil:
			switch free {
			case true:
				s.Free()
			}
		default:
			logs.Errorf(e.Error())
			s.Close()
			goto RETRY
		}
	default:
		logs.Errorf(err.Error())
	}
	return
}

func (s *client) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.GetResponse, e error) {
	return s.get(false, ctx, key, opts...)
}

func (s *client) GetRelease(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.GetResponse, e error) {
	return s.get(true, ctx, key, opts...)
}

func (s *client) get(free bool, ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.GetResponse, e error) {
RETRY:
	cli, err := s.get_cli()
	e = err
	switch err {
	case nil:
		logs.Errorf("")
		resp, e = cli.Get(ctx, key, opts...)
		switch e {
		case nil:
			switch free {
			case true:
				s.Free()
			}
		default:
			logs.Errorf(e.Error())
			s.Close()
			goto RETRY
		}
	default:
		logs.Errorf(err.Error())
	}
	return
}

func (s *client) Put(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, e error) {
	return s.put(false, ctx, key, val, opts...)
}

func (s *client) PutRelease(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, e error) {
	return s.put(true, ctx, key, val, opts...)
}

func (s *client) put(free bool, ctx context.Context, key string, val string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, e error) {
RETRY:
	cli, err := s.get_cli()
	e = err
	switch err {
	case nil:
		logs.Errorf("")
		resp, e = cli.Put(ctx, key, val, opts...)
		switch e {
		case nil:
			switch free {
			case true:
				s.Free()
			}
		default:
			logs.Errorf(e.Error())
			s.Close()
			goto RETRY
		}
	default:
		logs.Errorf(err.Error())
	}
	return
}

func (s *client) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) (c clientv3.WatchChan) {
	return s.watch(false, ctx, key, opts...)
}

func (s *client) WatchRelease(ctx context.Context, key string, opts ...clientv3.OpOption) (c clientv3.WatchChan) {
	return s.watch(true, ctx, key, opts...)
}

func (s *client) watch(free bool, ctx context.Context, key string, opts ...clientv3.OpOption) (c clientv3.WatchChan) {
	logs.Errorf("")
	cli, err := s.get_cli()
	switch err {
	case nil:
		switch cli {
		case nil:
			logs.Fatalf("error")
		default:
			logs.Errorf("")
			c = cli.Watch(ctx, key, opts...)
			switch free {
			case true:
				s.Free()
			}
		}
	default:
		logs.Errorf(err.Error())
	}
	return
}

func (s *client) free() {
	s.l.Lock()
	switch s.cli {
	case nil:
	default:
		logs.Errorf("")
		s.cli.Free()
		s.cli = nil
	}
	s.l.Unlock()
}

func (s *client) Free() {
	switch s.get_client() {
	case nil:
	default:
		s.free()
	}
}

func (s *client) close() {
	s.l.Lock()
	switch s.cli {
	case nil:
	default:
		logs.Errorf("")
		s.cli.Close()
		s.cli = nil
	}
	s.l.Unlock()
}

func (s *client) Close() {
	switch s.get_client() {
	case nil:
	default:
		s.close()
	}
}
