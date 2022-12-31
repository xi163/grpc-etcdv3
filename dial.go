package getcdv3

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cwloo/gonet/logs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
)

// BalanceDialAddr
func BalanceDialAddr(unique bool, schema, etcdAddr, serviceName, myAddr string, myPort int) (*grpc.ClientConn, error) {
	serviceValue := ""
	switch myAddr == "" || myPort == 0 {
	case true:
	default:
		serviceValue = strings.Join([]string{":", net.JoinHostPort(myAddr, strconv.Itoa(myPort))}, "")
	}
	return BalanceDial(unique, schema, etcdAddr, strings.Join([]string{serviceName, serviceValue}, ""))
}

// BalanceDialHost
func BalanceDialHost(unique bool, schema, etcdAddr, serviceName, myHost string) (*grpc.ClientConn, error) {
	serviceValue := ""
	switch myHost == "" {
	case true:
	default:
		serviceValue = strings.Join([]string{":", myHost}, "")
	}
	return BalanceDial(unique, schema, etcdAddr, strings.Join([]string{serviceName, serviceValue}, ""))
}

// BalanceDial
func BalanceDial(unique bool, schema, etcdAddr, serviceName string) (conn *grpc.ClientConn, err error) {
	target := TargetString(unique, schema, serviceName)
	b, ok := manager.GetAdd(schema)
	switch ok {
	case true:
		_, ok := b.GetAdd(target)
		switch ok {
		case true:
		}
		logs.Debugf("%v", target)
		ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
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
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	opts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"LoadBalancingPolicy": "%s"}`, roundrobin.Name)),
		grpc.WithChainStreamInterceptor(),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDisableRetry(),
	}
	target := strings.Join([]string{myAddr, strconv.Itoa(myPort)}, ":")
	logs.Debugf("%v", target)
	// conn, err = grpc.Dial(target, opts...)
	conn, err = grpc.DialContext(ctx, target, opts...)
	return
}

// DirectDialHost
func DirectDialHost(schema, etcdAddr, serviceName string, HostAddr string) (conn *grpc.ClientConn, err error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	opts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"LoadBalancingPolicy": "%s"}`, roundrobin.Name)),
		grpc.WithChainStreamInterceptor(),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDisableRetry(),
	}
	target := HostAddr
	logs.Debugf("%v", target)
	// conn, err = grpc.Dial(target, opts...)
	conn, err = grpc.DialContext(ctx, target, opts...)
	return
}
