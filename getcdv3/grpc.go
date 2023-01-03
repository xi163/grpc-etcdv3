package getcdv3

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/cwloo/gonet/logs"
	"github.com/cwloo/grpc-etcdv3/getcdv3/gRPCs"
	pb_public "github.com/cwloo/grpc-etcdv3/getcdv3/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// GetBalanceConn
func GetBalanceConn(schema, serviceName string) (conn gRPCs.ClientConn, e error) {
	target := TargetString(false, schema, serviceName)
	// logs.Debugf("%v %v:%v", target, "BalanceDial")
	c, err := BalanceDial(false, schema, serviceName)
	e = err
	switch err {
	case nil:
		client := pb_public.NewPeerClient(c)
		req := &pb_public.PeerReq{}
		resp, err := client.GetAddr(context.Background(), req)
		switch err {
		case nil:
			gRPCs.Conns().Update(target, map[string]bool{resp.Addr: true})
			clients, ok := gRPCs.Conns().GetAdd(target)
			switch ok {
			case true:
				conn = clients.AddConn(false, schema, serviceName, resp.Addr, BalanceDialHost, c)
			default:
				logs.Fatalf("error")
			}
		default:
			logs.Errorf(e.Error())
			conn = gRPCs.NewClientConn(c)
			return
		}
	default:
		logs.Errorf(err.Error())
	}
	return
}

// GetConn
func GetConn(schema, serviceName, myAddr string, myPort int) (conn gRPCs.ClientConn, err error) {
	conn, err = GetConnByHost(schema, serviceName, net.JoinHostPort(myAddr, strconv.Itoa(myPort)))
	return
}

// GetConn
func GetConnByHost(schema, serviceName, host string) (conn gRPCs.ClientConn, err error) {
	target := TargetString(false, schema, serviceName)
	gRPCs.Conns().Update(target, map[string]bool{host: true})
	clients, ok := gRPCs.Conns().GetAdd(target)
	switch ok {
	case true:
		logs.Debugf("%v %v:%v", target, "BalanceDialHost", host)
		clients.GetAdd(false, schema, serviceName, host, BalanceDialHost)
		conn, err = clients.GetConn(host)
	default:
		logs.Fatalf("error")
	}
	return
}

// GetConns
func GetConns(schema, serviceName string) (conns []gRPCs.ClientConn) {
	target := TargetString(false, schema, serviceName)
	// logs.Debugf("%v", target)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	// etcds.Update(etcdAddr, func(v Clientv3) {
	// 	v.Delete(ctx, target)
	// })
	cli := newClient(false)
	resp, err := cli.GetRelease(ctx, target, clientv3.WithPrefix())
	switch err {
	case nil:
		hosts := map[string]bool{}
		for i := range resp.Kvs {
			// logs.Debugf("%v %v => %v", target, string(resp.Kvs[i].Key), string(resp.Kvs[i].Value))
			hosts[string(resp.Kvs[i].Value)] = true
		}
		gRPCs.Conns().Update(target, hosts)
		clients, ok := gRPCs.Conns().GetAdd(target)
		switch ok {
		case true:
			conns, hosts = clients.GetConns(hosts)
			switch len(hosts) > 0 {
			case true:
				// directDial
				directDial := false
				switch directDial {
				case true:
					for host := range hosts {
						c, err := DirectDialHost(schema, serviceName, host)
						switch err {
						case nil:
							conns = append(conns, clients.AddConn(false, schema, serviceName, host, BalanceDialHost, c))
						default:
							logs.Errorf(err.Error())
							clients.Remove(host)
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
								for host, c := range m {
									// logs.Debugf("c=%v %v", i, host)
									conns = append(conns, clients.AddConn(false, schema, serviceName, host, BalanceDialHost, c))
								}
								return
							}
							// logs.Debugf("%v %v:%v", target, "BalanceDial")
							c, err := BalanceDial(false, schema, serviceName)
							switch err {
							case nil:
								client := pb_public.NewPeerClient(c)
								req := &pb_public.PeerReq{}
								resp, err := client.GetAddr(context.Background(), req)
								switch err {
								case nil:
									m[resp.Addr] = c
									switch len(m) == len(hosts) {
									case true:
										for host, c := range m {
											// logs.Debugf("c=%v %v", i, host)
											conns = append(conns, clients.AddConn(false, schema, serviceName, host, BalanceDialHost, c))
										}
										return
									}
								default:
									logs.Errorf(err.Error())
								}
							default:
								logs.Errorf(err.Error())
							}
						}
					default:
						for host := range hosts {
							logs.Debugf("%v %v:%v", target, "BalanceDialHost", host)
							clients.GetAdd(false, schema, serviceName, host, BalanceDialHost)
						}
						c, _ := clients.GetConns(hosts)
						conns = append(conns, c...)
					}
				}
			default:
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
