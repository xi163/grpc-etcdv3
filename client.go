package getcdv3

import (
	"context"
	"sync"

	"github.com/cwloo/gonet/logs"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	cli = newClient(false)
)

// <summary>
// Client
// <summary>
type Client struct {
	fixed bool
	cli   *Clientv3
	l     *sync.RWMutex
}

func newClient(fixed bool) *Client {
	s := &Client{fixed: fixed, l: &sync.RWMutex{}}
	return s
}

func (s *Client) Update(etcdAddr string, reset func(*Clientv3)) {
	etcds.update(etcdAddr, reset)
}

func (s *Client) get_client() (cli *Clientv3) {
	s.l.RLock()
	cli = s.cli
	s.l.RUnlock()
	return
}

func (s *Client) new_client() (cli *Clientv3, err error) {
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

func (s *Client) get() (cli *Clientv3, err error) {
	switch s.fixed {
	case true:
		cli = s.get_client()
		switch cli {
		case nil:
			cli, err = s.new_client()
		default:
		}
	default:
		cli, err = etcds.Get()
	}
	return
}

func (s *Client) put(cli *Clientv3) {
	switch s.fixed {
	case true:
	default:
		etcds.Put(cli)
	}
}

func (s *Client) Grant(ttl int64) (resp *clientv3.LeaseGrantResponse, e error) {
	n := 0
RETRY:
	cli, err := s.get()
	e = err
	switch err {
	case nil:
		resp, e = cli.Grant(ttl)
		switch e {
		case nil:
			s.put(cli)
		default:
			n++
			switch n <= RETRY_C {
			case true:
				goto RETRY
			default:
			}
		}
	default:
	}
	return
}

func (s *Client) GrantCtx(ctx context.Context, ttl int64) (resp *clientv3.LeaseGrantResponse, e error) {
	n := 0
RETRY:
	cli, err := s.get()
	e = err
	switch err {
	case nil:
		resp, e = cli.GrantCtx(ctx, ttl)
		switch e {
		case nil:
			s.put(cli)
		default:
			n++
			switch n <= RETRY_C {
			case true:
				goto RETRY
			default:
			}
		}
	default:
	}
	return
}

func (s *Client) KeepAlive(id clientv3.LeaseID) (ch <-chan *clientv3.LeaseKeepAliveResponse, e error) {
	n := 0
RETRY:
	cli, err := s.get()
	e = err
	switch err {
	case nil:
		ch, e = cli.KeepAlive(id)
		switch e {
		case nil:
			s.put(cli)
		default:
			n++
			switch n <= RETRY_C {
			case true:
				goto RETRY
			default:
			}
		}
	default:
	}
	return
}

func (s *Client) KeepAliveCtx(ctx context.Context, id clientv3.LeaseID) (ch <-chan *clientv3.LeaseKeepAliveResponse, e error) {
	n := 0
RETRY:
	cli, err := s.get()
	e = err
	switch err {
	case nil:
		ch, e = cli.KeepAliveCtx(ctx, id)
		switch e {
		case nil:
			s.put(cli)
		default:
			n++
			switch n <= RETRY_C {
			case true:
				goto RETRY
			default:
			}
		}
	default:
	}
	return
}

func (s *Client) Cancel() {
	switch s.fixed {
	case true:
		s.l.Lock()
		s.cli.cancel()
		s.l.Unlock()
	}
}

func (s *Client) Delete(key string, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, e error) {
	n := 0
RETRY:
	cli, err := s.get()
	e = err
	switch err {
	case nil:
		resp, e = cli.Delete(key, opts...)
		switch e {
		case nil:
			s.put(cli)
		default:
			n++
			switch n <= RETRY_C {
			case true:
				goto RETRY
			default:
			}
		}
	default:
	}
	return
}

func (s *Client) DeleteCtx(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, e error) {
	n := 0
RETRY:
	cli, err := s.get()
	e = err
	switch err {
	case nil:
		resp, e = cli.DeleteCtx(ctx, key, opts...)
		switch e {
		case nil:
			s.put(cli)
		default:
			n++
			switch n <= RETRY_C {
			case true:
				goto RETRY
			default:
			}
		}
	default:
	}
	return
}

func (s *Client) Get(key string, opts ...clientv3.OpOption) (resp *clientv3.GetResponse, e error) {
	n := 0
RETRY:
	cli, err := s.get()
	e = err
	switch err {
	case nil:
		resp, e = cli.Get(key, opts...)
		switch e {
		case nil:
			s.put(cli)
		default:
			n++
			switch n <= RETRY_C {
			case true:
				goto RETRY
			default:
			}
		}
	default:
	}
	return
}

func (s *Client) GetCtx(ctx context.Context, key string, opts ...clientv3.OpOption) (resp *clientv3.GetResponse, e error) {
	n := 0
RETRY:
	cli, err := s.get()
	e = err
	switch err {
	case nil:
		resp, e = cli.GetCtx(ctx, key, opts...)
		switch e {
		case nil:
			s.put(cli)
		default:
			n++
			switch n <= RETRY_C {
			case true:
				goto RETRY
			default:
			}
		}
	default:
	}
	return
}

func (s *Client) Put(key string, val string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, e error) {
	n := 0
RETRY:
	cli, err := s.get()
	e = err
	switch err {
	case nil:
		resp, e = cli.Put(key, val, opts...)
		switch e {
		case nil:
			s.put(cli)
		default:
			n++
			switch n <= RETRY_C {
			case true:
				goto RETRY
			default:
			}
		}
	default:
	}
	return
}

func (s *Client) PutCtx(ctx context.Context, key string, val string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, e error) {
	n := 0
RETRY:
	cli, err := s.get()
	e = err
	switch err {
	case nil:
		resp, e = cli.PutCtx(ctx, key, val, opts...)
		switch e {
		case nil:
			s.put(cli)
		default:
			n++
			switch n <= RETRY_C {
			case true:
				goto RETRY
			default:
			}
		}
	default:
	}
	return
}

func (s *Client) Watch(key string, opts ...clientv3.OpOption) (c clientv3.WatchChan) {
	logs.Errorf("")
	cli, err := s.get()
	switch err {
	case nil:
		switch cli {
		case nil:
			logs.Fatalf("error")
		default:
			c = cli.Watch(key, opts...)
		}
	default:
	}
	return
}

func (s *Client) WatchCtx(ctx context.Context, key string, opts ...clientv3.OpOption) (c clientv3.WatchChan) {
	logs.Errorf("")
	cli, err := s.get()
	switch err {
	case nil:
		switch cli {
		case nil:
			logs.Fatalf("error")
		default:
			c = cli.WatchCtx(ctx, key, opts...)
		}
	default:
	}
	return
}
