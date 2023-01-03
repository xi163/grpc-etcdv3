package getcdv3

import (
	"context"
	"sync"

	"github.com/cwloo/gonet/logs"
	clientv3 "go.etcd.io/etcd/client/v3"
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

func newClient(lock bool) Client {
	switch lock {
	case true:
		return &client{l: &sync.RWMutex{}}
	default:
		return &client{}
	}
}

func (s *client) get_client() (cli Clientv3) {
	switch s.l {
	case nil:
		cli = s.cli
	default:
		s.l.RLock()
		cli = s.cli
		s.l.RUnlock()
	}

	return
}

func (s *client) new_client_() (cli Clientv3, err error) {
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
	return
}

func (s *client) new_client() (cli Clientv3, err error) {
	switch s.l {
	case nil:
		cli, err = s.new_client_()
	default:
		s.l.Lock()
		cli, err = s.new_client_()
		s.l.Unlock()
	}
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
	cli, err := s.get_cli()
	switch err {
	case nil:
		switch cli {
		case nil:
			logs.Fatalf("error")
		default:
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

func (s *client) free_() {
	switch s.cli {
	case nil:
	default:
		s.cli.Free()
		s.cli = nil
	}
}

func (s *client) free() {
	switch s.l {
	case nil:
		s.free_()
	default:
		s.l.Lock()
		s.free_()
		s.l.Unlock()
	}
}

func (s *client) Free() {
	switch s.get_client() {
	case nil:
	default:
		s.free()
	}
}

func (s *client) close_() {
	switch s.cli {
	case nil:
	default:
		s.cli.Close()
		s.cli = nil
	}
}

func (s *client) close() {
	switch s.l {
	case nil:
		s.close_()
	default:
		s.l.Lock()
		s.close_()
		s.l.Unlock()
	}
}

func (s *client) Close() {
	switch s.get_client() {
	case nil:
	default:
		s.close()
	}
}
