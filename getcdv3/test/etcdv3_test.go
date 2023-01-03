package etcdv3_test

import (
	"testing"

	"github.com/cwloo/gonet/logs"
	getcdv3 "github.com/cwloo/grpc-etcdv3/getcdv3"
	"github.com/cwloo/grpc-etcdv3/getcdv3/gRPCs"
)

func TestMain(m *testing.M) {
	m.Run()
}
func Test(t *testing.T) {
	getcdv3.Update("192.168.0.113:2379")

	// getcdv3.RegisterEtcd("uploader", "file_server", "192.168.0.113", 5239, 10)

	conn, err := getcdv3.GetBalanceConn("uploader", "file_server")
	switch err {
	case nil:
		logs.Debugf("%v", conn.Conn().Target())
		conn.Free()
	default:
		logs.Errorf(err.Error())
	}

	conn, err = getcdv3.GetConn("uploader", "file_server", "192.168.0.113", 5239)
	switch err {
	case nil:
		logs.Debugf("%v", conn.Conn().Target())
		conn.Free()
	default:
		logs.Errorf(err.Error())
	}

	rpcConns := getcdv3.GetConns("uploader", "file_server")
	logs.Debugf("len=%v", len(rpcConns))
	for _, conn := range rpcConns {
		conn.Free()
	}
	gRPCs.Conns().Close()
	logs.Close()
}
