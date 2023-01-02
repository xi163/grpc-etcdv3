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
	cli Client
}

var (
	register = Register{
		cli: newClient(),
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
	// ctx, _ := context.WithCancel(context.Background())
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	etcds.Update(etcdAddr, func(v Clientv3) {
		v.Delete(ctx, serviceKey)
	})
	gresp, err := register.cli.Grant(ctx, int64(ttl))
	switch err {
	case nil:
		_, err := register.cli.Put(ctx, serviceKey, serviceValue, clientv3.WithLease(gresp.ID))
		switch err {
		case nil:
			kresp, err := register.cli.KeepAlive(ctx, gresp.ID)
			switch err {
			case nil:
				// logs.Infof("RegisterEtcd ok %v", args)
				go func() {
					for {
						select {
						case pv, ok := <-kresp:
							switch ok {
							case true:
								// logs.Debugf("KeepAlive ok %v %v", pv, args)
							default:
								logs.Errorf("KeepAlive failed %v %v", pv, args)
								t := time.NewTicker(time.Duration(ttl/2) * time.Second)
								for {
									select {
									case <-t.C:
									}
									// ctx, _ := context.WithCancel(context.Background())
									ctx, _ := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
									gresp, err := register.cli.Grant(ctx, int64(ttl))
									switch err {
									case nil:
										_, err := register.cli.PutRelease(ctx, serviceKey, serviceValue, clientv3.WithLease(gresp.ID))
										switch err {
										case nil:
										default:
											logs.Errorf("%v %v %v", err.Error(), args, gresp.ID)
										}
									default:
										logs.Errorf("%v %v", err.Error(), args)
									}
								}
							}
						}
					}
				}()
			default:
				logs.Errorf("KeepAlive %v %v %v", err.Error(), args, gresp.ID)
			}
		default:
			logs.Errorf("%v %v %v", err.Error(), args, gresp.ID)
		}
	default:
		logs.Errorf("%v %v", err.Error(), ttl)
		return errors.New(logs.SprintErrorf(err.Error()))
	}
	return nil
}
