package getcdv3

import (
	"context"
	"runtime"
	"strings"
	"sync"

	"github.com/cwloo/gonet/core/base/cc"
	"github.com/cwloo/gonet/core/base/mq"
	"github.com/cwloo/gonet/core/base/mq/lq"
	"github.com/cwloo/gonet/logs"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	grpc_resolver "google.golang.org/grpc/resolver"
)

// <summary>
// ExitStruct
// <summary>
type ExitStruct struct {
	schema string
	target string
}

// <summary>
// WatcherMsg
// <summary>
type WatcherMsg struct {
	schema string
	target string
	cc     grpc_resolver.ClientConn
	hosts  map[string]bool
}

// <summary>
// Watcher
// <summary>
type Watcher struct {
	// we can't inherit from grpc_resolver.Resolver, if do so,
	// grpc_resolver.Resolver.Close() will destroy Watcher, then it will be unpredictable !
	r        *Resolver
	cli      Client
	revision int64
	target   string
	cb       func(string)
	watched  bool
	mq       mq.Queue
	l        *sync.Mutex
	cond     *sync.Cond
	stopping cc.Singal
	flag     cc.AtomFlag
	hosts    map[string]bool
	cc       map[grpc_resolver.ClientConn]bool
}

func newWatcher(target string, cb func(string)) *Watcher {
	logs.Tracef("%v", target)
	s := &Watcher{
		r:        newResolver(target),
		cli:      newClient(),
		cb:       cb,
		target:   target,
		l:        &sync.Mutex{},
		flag:     cc.NewAtomFlag(),
		stopping: cc.NewSingal(),
		hosts:    map[string]bool{},
		cc:       map[grpc_resolver.ClientConn]bool{},
	}
	s.cond = sync.NewCond(s.l)
	s.sched()
	return s
}

func (s *Watcher) reset() {
	s.cb(s.target)
	s.mq = nil
}

func (s *Watcher) Put() {
	s.reset()
}

func (s *Watcher) Close() {
	logs.Errorf("%v", s.target)
	switch s.watched {
	case false:
		s.Put()
	default:
		s.stopping.Signal()
		s.wait_stop()
	}
}

func (s *Watcher) NotifyClose() {
	logs.Errorf("%v", s.target)
	switch s.watched {
	case false:
		s.Put()
	default:
		s.stopping.Signal()
	}
}

func (s *Watcher) onQuit() {
	logs.Errorf("%v", s.target)
	s.Put()
}

func (s *Watcher) Watch(msg *WatcherMsg) {
	switch len(msg.hosts) > 0 {
	case true:
		s.sched()
		s.mq.Push(msg)
	}
}

func (s *Watcher) run() {
	s.l.Lock()
	s.watched = true
	s.cond.Signal()
	s.l.Unlock()

	s.running()
	s.onQuit()

	s.l.Lock()
	s.watched = false
	s.cond.Signal()
	s.l.Unlock()
}

func (s *Watcher) running() (exit bool) {
	i, t := 0, 200
LOOP:
	for {
		if i > t {
			i = 0
			runtime.GC()
		}
		i++
		exit := s.handler()
		switch exit {
		case true:
			break LOOP
		}
	}
	return
}

func (s *Watcher) handler() (exit bool) {
	exit = s.watching()
	switch exit {
	case true:
		logs.Tracef("--------------------- ****** exit %v", s.target)
	}
	return
}

func (s *Watcher) watching() (exit bool) {
	exit = s.watch_handler(s.cli.WatchRelease(
		context.Background(),
		s.target,
		clientv3.WithPrefix(),
		clientv3.WithPrefix()))
	logs.Warnf("end %v", s.target)
	return
}

func (s *Watcher) Pick() (exit bool) {
	v := s.mq.Pick()
	for _, msg := range v {
		switch msg := msg.(type) {
		case *ExitStruct:
			logs.Errorf("%v", msg.target)
			goto EXIT
		case *WatcherMsg:
			switch s.target == msg.target {
			case true:
			default:
				logs.Fatalf("error")
			}
			for host := range msg.hosts {
				s.hosts[host] = true
			}
			s.cc[msg.cc] = true
		}
	}
EXIT:
	exit = true
	return
}

func (s *Watcher) watch_handler(watchChan clientv3.WatchChan) (exit bool) {
	select { // block here
	case resp := <-watchChan:
		logs.Warnf("begin %v", s.target)
		EXIT := s.Pick()
		for _, ev := range resp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				_, ok := s.hosts[string(ev.Kv.Value)]
				switch ok {
				case true:
					logs.Errorf("<PUT> %v builder:%+v watch:%v", s.target, s.hosts, string(ev.Kv.Value))
				default:
					logs.Debugf("<PUT> %v builder:%+v watch:%v", s.target, s.hosts, string(ev.Kv.Value))
					hosts := []grpc_resolver.Address{}
					for host := range s.hosts {
						hosts = append(hosts, grpc_resolver.Address{Addr: host})
					}
					for cc := range s.cc {
						cc.UpdateState(grpc_resolver.State{Addresses: hosts})
					}
					s.hosts[string(ev.Kv.Value)] = true
				}
			case mvccpb.DELETE:
				i := strings.LastIndexAny(string(ev.Kv.Key), "/")
				switch i >= 0 {
				case true:
					host := string(ev.Kv.Key)[i+1:]
					_, ok := s.hosts[host]
					switch ok {
					case true:
						logs.Debugf("<DELETE> %v %v => %v", s.target, string(ev.Kv.Key), host)
						rpcConns.RemoveConn(host)
						delete(s.hosts, host)
						hosts := []grpc_resolver.Address{}
						for host := range s.hosts {
							hosts = append(hosts, grpc_resolver.Address{Addr: host})
						}
						for cc := range s.cc {
							cc.UpdateState(grpc_resolver.State{Addresses: hosts})
						}
						s.cb(s.target)
					default:
						logs.Errorf("<DELETE> %v %v => %v", s.target, string(ev.Kv.Key), host)
						rpcConns.RemoveConn(host)
						delete(s.hosts, host)
						hosts := []grpc_resolver.Address{}
						for host := range s.hosts {
							hosts = append(hosts, grpc_resolver.Address{Addr: host})
						}
						for cc := range s.cc {
							cc.UpdateState(grpc_resolver.State{Addresses: hosts})
						}
						s.cb(s.target)
					}
				default:
					host := string(ev.Kv.Key)
					logs.Errorf("<DELETE> %v %v => %v", s.target, string(ev.Kv.Key), host)
				}
			default:
			}
		}
		switch EXIT {
		case true:
			s.stopping.Signal()
		}
	case <-s.stopping.Read():
		logs.Debugf("stopping %v", s.target)
		goto EXIT
		// default:
	}
	return
EXIT:
	exit = true
	return
}

func (s *Watcher) sched() {
	switch s.watched {
	case true:
	default:
		switch s.flag.TestSet() {
		case true:
			logs.Tracef("--------------------- ****** %v", s.target)
			s.mq = lq.NewList(1000)
			go s.run()
			s.wait()
			s.flag.Reset()
		default:
		}
	}
}

func (s *Watcher) wait() {
	s.l.Lock()
	for !s.watched {
		s.cond.Wait()
	}
	s.l.Unlock()
}

func (s *Watcher) wait_stop() {
	s.l.Lock()
	for s.watched {
		s.cond.Wait()
	}
	s.l.Unlock()
}
