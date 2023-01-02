##### golang grpc-etcdv3

###### 1. etcd conn pool

###### 2. grpc conn pool


#### interface  

##### etcd
* Auth(username, password string)
* Update(etcdAddr string) 

##### register etcd
* RegisterEtcd(schema, serviceName, myAddr string, myPort int, ttl int) error


##### get random grpc conn for banlance 

* GetBalanceConn(schema, serviceName string) (conn *grpc.ClientConn, err error)


##### get special grpc conn indicate ip, port

* GetConn(schema, serviceName string, myAddr string, myPort int) (conn *grpc.ClientConn, err error)


##### Get all grpc conns indicate serviceName

* GetConns(schema, serviceName string) (conns []*grpc.ClientConn)