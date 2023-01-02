package getcdv3

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/cwloo/gonet/logs"
	pb_public "github.com/cwloo/grpc-etcdv3/getcdv3/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// GetBalanceConn
func GetBalanceConn(schema, serviceName string) (conn *grpc.ClientConn, err error) {
	target := TargetString(false, schema, serviceName)
	// logs.Debugf("%v %v:%v", target, "BalanceDial")
	conn, err = BalanceDial(false, schema, serviceName)
	switch err {
	case nil:
		client := pb_public.NewPeerClient(conn)
		req := &pb_public.PeerReq{}
		resp, e := client.GetAddr(context.Background(), req)
		switch e {
		case nil:
			hostConn, ok := rpcConns.GetAdd(target)
			switch ok {
			case true:
				hostConn.TryAdd(resp.Addr, conn)
			}
		default:
			logs.Errorf(e.Error())
			return
		}
	default:
		logs.Errorf(err.Error())
	}
	return
}

// GetConn
func GetConn(schema, serviceName, myAddr string, myPort int) (conn *grpc.ClientConn, err error) {
	return GetConnByHost(schema, serviceName, net.JoinHostPort(myAddr, strconv.Itoa(myPort)))
}

// GetConn
func GetConnByHost(schema, serviceName, myHost string) (conn *grpc.ClientConn, err error) {
	target := TargetString(false, schema, serviceName)
	hostConn, ok := rpcConns.GetAdd(target)
	switch ok {
	case true:
		conn, ok = hostConn.Get(myHost)
		switch ok {
		case true:
			// logs.Debugf("%v %v:%v", target, "BalanceDialHost", myHost)
			return conn, nil
		default:
			// logs.Debugf("%v %v:%v", target, "BalanceDialHost", myHost)
			conn, err = BalanceDialHost(false, schema, serviceName, myHost)
			switch err {
			case nil:
				hostConn.TryAdd(myHost, conn)
			default:
				logs.Errorf(err.Error())
				hostConn.Remove(myHost)
			}
		}
	}
	return
}

// GetConns
func GetConns(schema, serviceName string) (conns []*grpc.ClientConn) {
	target := TargetString(false, schema, serviceName)
	// logs.Debugf("%v", target)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	// etcds.Update(etcdAddr, func(v Clientv3) {
	// 	v.Delete(ctx, target)
	// })
	resp, err := cli.GetRelease(ctx, target, clientv3.WithPrefix())
	switch err {
	case nil:
		hosts := map[string]bool{}
		for i := range resp.Kvs {
			// logs.Debugf("%v %v => %v", target, string(resp.Kvs[i].Key), string(resp.Kvs[i].Value))
			hosts[string(resp.Kvs[i].Value)] = true
		}
		// array := hosts
		rpcConns.Update(target, hosts)
		hostConn, ok := rpcConns.GetAdd(target)
		switch ok {
		case true:
			conns, hosts = hostConn.GetConns(hosts)
			switch len(hosts) > 0 {
			case true:
				// directDial
				directDial := false
				switch directDial {
				case true:
					for host := range hosts {
						r, err := DirectDialHost(schema, serviceName, host)
						switch err {
						case nil:
							conns = append(conns, r)
							hostConn.TryAdd(host, r)
						default:
							logs.Errorf(err.Error())
							hostConn.Remove(host)
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
									hostConn.TryAdd(addr, r)
								}
								return
							}
							// logs.Debugf("%v %v:%v", target, "BalanceDial")
							r, err := BalanceDial(false, schema, serviceName)
							switch err {
							case nil:
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
										// logs.Debugf("c=%v %v", i, addr)
										conns = append(conns, r)
										hostConn.TryAdd(addr, r)
									}
									return
								}
							default:
								logs.Errorf(err.Error())
							}
						}
					default:
						for host := range hosts {
							// logs.Debugf("%v %v:%v", target, "BalanceDialHost", host)
							r, err := BalanceDialHost(false, schema, serviceName, host)
							switch err {
							case nil:
								conns = append(conns, r)
								hostConn.TryAdd(host, r)
							default:
								logs.Errorf(err.Error())
								hostConn.Remove(host)
							}
						}
					}
				}
			default:
				// logs.Debugf("%v %v%v", target, "GetConnsByHost", Slice(array))
			}
		}
	default:
		logs.Errorf(err.Error())
		return
	}
	return
}

// schema:///node/ip:port
// func GetConnHost4Unique(schema, serviceName, myAddr string) (*grpc.ClientConn, error) {
// 	return BalanceDial(true, schema, strings.Join([]string{serviceName, myAddr}, "/"))
// }
