package main

import (
	"context"
	"sync"
	"time"

	"github.com/cwloo/gonet/logs"
	"github.com/cwloo/grpc-etcdv3/getcdv3"
	"github.com/cwloo/grpc-etcdv3/getcdv3/gRPCs"
	pb_getcdv3 "github.com/cwloo/grpc-etcdv3/getcdv3/proto"
)

func main() {
	getcdv3.Update("192.168.0.113:2379")

	// getcdv3.RegisterEtcd("uploader", "file_server", "192.168.0.113", 5239, 10)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			for {
				time.Sleep(3 * time.Second)
				conn, err := getcdv3.GetBalanceConn("uploader", "file_server")
				switch err {
				case nil:
					// logs.Debugf("%v", conn.Conn().Target())
					conn.Free()
				default:
					logs.Errorf(err.Error())
				}

				conn, err = getcdv3.GetConn("uploader", "file_server", "192.168.0.113", 5239)
				switch err {
				case nil:
					// logs.Debugf("%v", conn.Conn().Target())
					conn.Free()
				default:
					logs.Errorf(err.Error())
				}

				rpcConns := getcdv3.GetConns("uploader", "file_server")
				// logs.Debugf("len=%v", len(rpcConns))
				for _, v := range rpcConns {
					client := pb_getcdv3.NewPeerClient(v.Conn())
					req := &pb_getcdv3.PeerReq{}
					_, err := client.GetAddr(context.Background(), req)
					switch err {
					case nil:
						v.Free()
					default:
						logs.Errorf(err.Error())
						gRPCs.Conns().RemoveBy(err)
						v.Close()
					}
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	gRPCs.Conns().Close()
	logs.Close()
}
