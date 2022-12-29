package getcdv3

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cwloo/gonet/logs"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// <summary>
// Register
// <summary>
type Register struct {
	cli *Client
}

var (
	register = Register{
		cli: newClient(true),
	}
)

// "%s:///%s/"
func GetPrefix(schema, serviceName string) string {
	return fmt.Sprintf("%s:///%s/", schema, serviceName)
}

// "%s:///%s"
func GetPrefix4Unique(schema, serviceName string) string {
	return fmt.Sprintf("%s:///%s", schema, serviceName)
}

func RegisterEtcd(schema, etcdAddr, myAddr string, myPort int, serviceName string, ttl int) error {
	err := registerEtcd(schema, etcdAddr, myAddr, myPort, serviceName, ttl)
	if err != nil {
		return err
	}
	serviceName = strings.Join([]string{serviceName, net.JoinHostPort(myAddr, strconv.Itoa(myPort))}, ":")
	err = registerEtcd(schema, etcdAddr, myAddr, myPort, serviceName, ttl)
	if err != nil {
		return err
	}
	return nil
}

func GetTarget(schema, serviceName string) string {
	return GetPrefix(schema, serviceName)
}

func GetUniqueTarget(schema, serviceName, myAddr string, myPort int) string {
	return strings.Join([]string{GetPrefix4Unique(schema, serviceName), ":", net.JoinHostPort(myAddr, strconv.Itoa(myPort)), "/"}, "")
}

func registerEtcd(schema, etcdAddr, myAddr string, myPort int, serviceName string, ttl int) error {
	serviceValue := net.JoinHostPort(myAddr, strconv.Itoa(myPort))
	serviceKey := GetPrefix(schema, serviceName) + serviceValue
	args := strings.Join([]string{schema, etcdAddr, serviceName, net.JoinHostPort(myAddr, strconv.Itoa(myPort))}, " ")
	ttl = ttl * 3
	register.cli.Update(etcdAddr, func(v *Clientv3) {
		v.Cancel()
		v.Delete(serviceKey)
	})
	resp, err := register.cli.Grant(int64(ttl))
	if err != nil {
		logs.Errorf("%v %v", err.Error(), ttl)
		return errors.New(logs.SprintErrorf(err.Error()))
	}
	// logs.Infof("Grant ok %v", resp.ID)
	if _, err = register.cli.Put(serviceKey, serviceValue, clientv3.WithLease(resp.ID)); err != nil {
		logs.Errorf("%v %v %v", err.Error(), args, resp.ID)
		return errors.New(logs.SprintErrorf("%v %v %v", err, serviceKey, serviceValue))
	}
	kresp, err := register.cli.KeepAlive(resp.ID)
	if err != nil {
		logs.Errorf("KeepAlive %v %v %v", err.Error(), args, resp.ID)
		return errors.New(logs.SprintErrorf("keepalive %v %v", err.Error(), resp.ID))
	}
	// logs.Infof("RegisterEtcd ok %v", args)
	go func() {
		for {
			select {
			case pv, ok := <-kresp:
				if ok {
					// logs.Debugf("KeepAlive ok %v %v", pv, args)
				} else {
					logs.Errorf("KeepAlive failed %v %v", pv, args)
					t := time.NewTicker(time.Duration(ttl/2) * time.Second)
					for {
						select {
						case <-t.C:
						}
						ctx, _ := context.WithCancel(context.Background())
						resp, err := register.cli.GrantCtx(ctx, int64(ttl))
						if err != nil {
							logs.Errorf("%v %v", err.Error(), args)
							continue
						}
						if _, err := register.cli.Put(serviceKey, serviceValue, clientv3.WithLease(resp.ID)); err != nil {
							logs.Errorf("%v %v %v", err.Error(), args, resp.ID)
							continue
						} else {
							logs.Infof("etcd Put ok %v %v", args, resp.ID)
						}
					}
				}
			}
		}
	}()
	return nil
}
