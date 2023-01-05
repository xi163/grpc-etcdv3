package getcdv3

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
)

// BalanceDialAddr
func BalanceDialAddr(unique bool, schema, serviceName, addr string, port int) (*grpc.ClientConn, error) {
	switch addr == "" || port == 0 {
	case true:
		return BalanceDial(unique, schema, serviceName)
	default:
		return BalanceDialHost(unique, schema, serviceName, net.JoinHostPort(addr, strconv.Itoa(port)))
	}
}

// BalanceDialHost
func BalanceDialHost(unique bool, schema, serviceName, host string) (*grpc.ClientConn, error) {
	switch host == "" {
	case true:
		return BalanceDial(unique, schema, serviceName)
	default:
		return BalanceDial(unique, schema, strings.Join([]string{serviceName, host}, ":"))
	}
}

// BalanceDial
func BalanceDial(unique bool, schema, serviceName string) (conn *grpc.ClientConn, err error) {
	target := TargetString(unique, schema, serviceName)
	_, ok := builders.GetAdd(schema)
	switch ok {
	case true:
		// logs.Debugf("%v", target)
		ctx, _ := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
		// etcds.Update(etcdAddr, func(v Clientv3) {
		// 	v.Delete(ctx, target)
		// })
		opts := []grpc.DialOption{
			grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"LoadBalancingPolicy": "%s"}`, roundrobin.Name)),
			grpc.WithChainStreamInterceptor(),
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithDisableRetry(),
		}
		// Resolver.Build
		// conn, err = grpc.Dial(b.target, opts...)
		conn, err = grpc.DialContext(ctx, target, opts...)
	}
	return
}

// DirectDialAddr
func DirectDialAddr(schema, serviceName, addr string, port int) (conn *grpc.ClientConn, err error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	opts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"LoadBalancingPolicy": "%s"}`, roundrobin.Name)),
		grpc.WithChainStreamInterceptor(),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDisableRetry(),
	}
	target := strings.Join([]string{addr, strconv.Itoa(port)}, ":")
	// logs.Debugf("%v", target)
	// conn, err = grpc.Dial(target, opts...)
	conn, err = grpc.DialContext(ctx, target, opts...)
	return
}

// DirectDialHost
func DirectDialHost(schema, serviceName string, HostAddr string) (conn *grpc.ClientConn, err error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	opts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"LoadBalancingPolicy": "%s"}`, roundrobin.Name)),
		grpc.WithChainStreamInterceptor(),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDisableRetry(),
	}
	target := HostAddr
	// logs.Debugf("%v", target)
	// conn, err = grpc.Dial(target, opts...)
	conn, err = grpc.DialContext(ctx, target, opts...)
	return
}
