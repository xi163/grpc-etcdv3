package getcdv3

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/cwloo/gonet/logs"
	pb_public "github.com/cwloo/uploader/proto/public"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// GetBalanceConn
func GetBalanceConn(schema, etcdaddr, serviceName string) (conn *grpc.ClientConn, err error) {
	target := TargetString(false, schema, serviceName)
	logs.Debugf("%v %v:%v", target, "BalanceDial")
	conn, err = BalanceDial(false, schema, etcdaddr, serviceName)
	switch err {
	case nil:
		client := pb_public.NewPeerClient(conn)
		req := &pb_public.PeerReq{}
		resp, e := client.GetAddr(context.Background(), req)
		switch e {
		case nil:
			rpcConns.AddConnByHost(resp.Addr, conn)
		default:
			logs.Errorf(e.Error())
			return
		}
	default:
	}
	return
}

// GetConn
func GetConn(schema, etcdaddr, serviceName string, myAddr string, myPort int) (conn *grpc.ClientConn, err error) {
	conn, ok := rpcConns.GetConnByAddr(myAddr, myPort)
	switch ok {
	case true:
		target := TargetString(false, schema, serviceName)
		logs.Debugf("%v %v:%v", target, "GetConnByAddr", net.JoinHostPort(myAddr, myAddr))
		return conn, nil
	default:
		target := TargetString(false, schema, serviceName)
		logs.Debugf("%v %v:%v", target, "BalanceDialAddr", net.JoinHostPort(myAddr, myAddr))
		conn, err = BalanceDialAddr(false, schema, etcdaddr, serviceName, myAddr, myPort)
		switch err {
		case nil:
			rpcConns.AddConnByAddr(myAddr, myPort, conn)
		default:
		}
	}
	return
}

// GetConns
func GetConns(schema, etcdaddr, serviceName string) (conns []*grpc.ClientConn) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(etcdaddr, ",")})
	if err != nil {
		logs.Errorf(err.Error())
		return nil
	}
	target := TargetString(false, schema, serviceName)
	logs.Debugf("%v", target)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := cli.Get(ctx, target, clientv3.WithPrefix())
	hosts := []string{}
	switch err {
	case nil:
		for i := range resp.Kvs {
			// logs.Debugf("%v %v => %v", target, string(resp.Kvs[i].Key), string(resp.Kvs[i].Value))
			hosts = append(hosts, string(resp.Kvs[i].Value))
		}
	default:
		cli.Close()
		logs.Errorf(err.Error())
		return nil
	}
	cli.Close()
	array := hosts
	conns, hosts = rpcConns.GetConnsByHost(hosts)
	switch len(hosts) > 0 {
	case true:
		// directDial
		directDial := false
		switch directDial {
		case true:
			for _, host := range hosts {
				r, err := DirectDialHost(schema, etcdaddr, serviceName, host)
				switch err {
				case nil:
					conns = append(conns, r)
					rpcConns.AddConnByHost(host, r)
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
							rpcConns.AddConnByHost(addr, r)
						}
						return
					}
					target := TargetString(false, schema, serviceName)
					logs.Debugf("%v %v:%v", target, "BalanceDial")
					r, _ := BalanceDial(false, schema, etcdaddr, serviceName)
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
							rpcConns.AddConnByHost(addr, r)
						}
						return
					}
				}
			default:
				for _, host := range hosts {
					logs.Debugf("%v %v:%v", target, "BalanceDialHost", host)
					r, _ := BalanceDialHost(false, schema, etcdaddr, serviceName, host)
					conns = append(conns, r)
					rpcConns.AddConnByHost(host, r)
				}
			}
		}
	default:
		logs.Debugf("%v %v%v", target, "GetConnsByHost", array)
	}
	return
}

// schema:///node/ip:port
// func GetConnHost4Unique(schema, etcdaddr, serviceName, myAddr string) (*grpc.ClientConn, error) {
// 	return BalanceDial(true, schema, etcdaddr, strings.Join([]string{serviceName, myAddr}, "/"))
// }
