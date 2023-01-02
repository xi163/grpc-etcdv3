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
func BalanceDialAddr(unique bool, schema, etcdAddr, serviceName, myAddr string, myPort int) (*grpc.ClientConn, error) {
	switch myAddr == "" || myPort == 0 {
	case true:
		return BalanceDial(unique, schema, etcdAddr, serviceName)
	default:
		return BalanceDialHost(unique, schema, etcdAddr, serviceName, net.JoinHostPort(myAddr, strconv.Itoa(myPort)))
	}
}

// BalanceDialHost
func BalanceDialHost(unique bool, schema, etcdAddr, serviceName, myHost string) (*grpc.ClientConn, error) {
	switch myHost == "" {
	case true:
		return BalanceDial(unique, schema, etcdAddr, serviceName)
	default:
		return BalanceDial(unique, schema, etcdAddr, strings.Join([]string{serviceName, myHost}, ":"))
	}
}

// BalanceDial
func BalanceDial(unique bool, schema, etcdAddr, serviceName string) (conn *grpc.ClientConn, err error) {
	target := TargetString(unique, schema, serviceName)
	_, ok := manager.GetAdd(schema)
	switch ok {
	case true:
		// logs.Debugf("%v", target)
		ctx, _ := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
		etcds.Update(etcdAddr, func(v Clientv3) {
			v.Delete(ctx, target)
		})
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
func DirectDialAddr(schema, etcdAddr, serviceName, myAddr string, myPort int) (conn *grpc.ClientConn, err error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	opts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"LoadBalancingPolicy": "%s"}`, roundrobin.Name)),
		grpc.WithChainStreamInterceptor(),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDisableRetry(),
	}
	target := strings.Join([]string{myAddr, strconv.Itoa(myPort)}, ":")
	// logs.Debugf("%v", target)
	// conn, err = grpc.Dial(target, opts...)
	conn, err = grpc.DialContext(ctx, target, opts...)
	return
}

// DirectDialHost
func DirectDialHost(schema, etcdAddr, serviceName string, HostAddr string) (conn *grpc.ClientConn, err error) {
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
