##### golang grpc-etcdv3

###### 1. etcd conn pool

###### 2. grpc conn pool


#### interface  

##### etcd
* Auth(username, password string)
* Update(etcdAddr string) 

##### register etcd
* RegisterEtcd(schema, serviceName, addr string, port int, ttl int) error


##### get random grpc conn for balance 

* GetBalanceConn(schema, serviceName string) (conn gRPCs.Client, err error)


##### get special grpc conn indicate ip, port

* GetConn(schema, serviceName, addr string, port int) (conn gRPCs.Client, err error) 

* GetConnByHost(schema, serviceName, host string) (conn gRPCs.Client, err error)

##### Get all grpc conns indicate serviceName

* GetConns(schema, serviceName string) (conns []gRPCs.Client)