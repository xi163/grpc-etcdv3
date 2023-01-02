package getcdv3

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/cwloo/gonet/logs"
	pb_public "github.com/cwloo/uploader/proto/public"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// GetBalanceConn
func GetBalanceConn(schema, etcdAddr, serviceName string) (conn *grpc.ClientConn, err error) {
	target := TargetString(false, schema, serviceName)

	logs.Debugf("%v %v:%v", target, "BalanceDial")
	conn, err = BalanceDial(false, schema, etcdAddr, serviceName)
	switch err {
	case nil:
		client := pb_public.NewPeerClient(conn)
		req := &pb_public.PeerReq{}
		resp, e := client.GetAddr(context.Background(), req)
		switch e {
		case nil:
			rpcConns.TryAdd(resp.Addr, conn)
		default:
			logs.Errorf(e.Error())
			return
		}
	default:
	}
	return
}

// GetConn
func GetConn(schema, etcdAddr, serviceName, myAddr string, myPort int) (conn *grpc.ClientConn, err error) {
	return GetConnByHost(schema, etcdAddr, serviceName, net.JoinHostPort(myAddr, strconv.Itoa(myPort)))
}

// GetConn
func GetConnByHost(schema, etcdAddr, serviceName, myHost string) (conn *grpc.ClientConn, err error) {
	target := TargetString(false, schema, serviceName)
	conn, ok := rpcConns.Get(myHost)
	switch ok {
	case true:
		logs.Debugf("%v %v:%v", target, "BalanceDialHost", myHost)
		return conn, nil
	default:
		logs.Debugf("%v %v:%v", target, "BalanceDialHost", myHost)
		conn, err = BalanceDialHost(false, schema, etcdAddr, serviceName, myHost)
		switch err {
		case nil:
			rpcConns.TryAdd(myHost, conn)
		default:
		}
	}
	return
}

// GetConns
func GetConns(schema, etcdAddr, serviceName string) (conns []*grpc.ClientConn) {
	target := TargetString(false, schema, serviceName)
	logs.Debugf("%v", target)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	etcds.Update(etcdAddr, func(v Clientv3) {
		v.Delete(ctx, target)
	})
	cli := newClient()
	resp, err := cli.GetRelease(ctx, target, clientv3.WithPrefix())
	switch err {
	case nil:
		hosts := []string{}
		for i := range resp.Kvs {
			// logs.Debugf("%v %v => %v", target, string(resp.Kvs[i].Key), string(resp.Kvs[i].Value))
			hosts = append(hosts, string(resp.Kvs[i].Value))
		}
		array := hosts
		conns, hosts = rpcConns.GetConns(hosts)
		switch len(hosts) > 0 {
		case true:
			// directDial
			directDial := false
			switch directDial {
			case true:
				for _, host := range hosts {
					r, err := DirectDialHost(schema, etcdAddr, serviceName, host)
					switch err {
					case nil:
						conns = append(conns, r)
						rpcConns.TryAdd(host, r)
					default:
						logs.Errorf(err.Error())
					}
				}
			default:
				// banlanceDial
				banlanceDial := false
				switch banlanceDial {
				case true:
					i := 0
					m := map[string]*grpc.ClientConn{}
					for {
						i++
						switch i >= 20 {
						case true:
							for addr, r := range m {
								logs.Debugf("c=%v %v", i, addr)
								conns = append(conns, r)
								rpcConns.TryAdd(addr, r)
							}
							return
						}
						target := TargetString(false, schema, serviceName)
						logs.Debugf("%v %v:%v", target, "BalanceDial")
						r, _ := BalanceDial(false, schema, etcdAddr, serviceName)
						client := pb_public.NewPeerClient(r)
						req := &pb_public.PeerReq{}
						resp, err := client.GetAddr(context.Background(), req)
						if err != nil {
							logs.Errorf(err.Error())
							continue
						}
						m[resp.Addr] = r
						if len(m) == len(hosts) {
							for addr, r := range m {
								logs.Debugf("c=%v %v", i, addr)
								conns = append(conns, r)
								rpcConns.TryAdd(addr, r)
							}
							return
						}
					}
				default:
					for _, host := range hosts {
						logs.Debugf("%v %v:%v", target, "BalanceDialHost", host)
						r, _ := BalanceDialHost(false, schema, etcdAddr, serviceName, host)
						conns = append(conns, r)
						rpcConns.TryAdd(host, r)
					}
				}
			}
		default:
			logs.Debugf("%v %v%v", target, "GetConnsByHost", array)
		}
	default:
		logs.Errorf(err.Error())
		return nil
	}
	return
}

// schema:///node/ip:port
// func GetConnHost4Unique(schema, etcdAddr, serviceName, myAddr string) (*grpc.ClientConn, error) {
// 	return BalanceDial(true, schema, etcdAddr, strings.Join([]string{serviceName, myAddr}, "/"))
// }
