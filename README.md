##### golang grpc-etcdv3

###### 1. etcd conn pool

###### 2. grpc conn pool


#### interface  

##### register etcd
* registerEtcd(schema, etcdAddr, myAddr string, myPort int, serviceName string, ttl int) error


##### get random grpc conn for banlance 

* GetBalanceConn(schema, etcdaddr, serviceName string) (conn *grpc.ClientConn, err error)


##### get special grpc conn indicate ip, port

* GetConn(schema, etcdaddr, serviceName string, myAddr string, myPort int) (conn *grpc.ClientConn, err error)


##### Get all grpc conns indicate serviceName

* GetConns(schema, etcdaddr, serviceName string) (conns []*grpc.ClientConn)