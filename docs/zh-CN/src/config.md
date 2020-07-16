# 配置文件

前面我们已经学会了，如果以最基本的方式启动chubaodb。下面我们来学一种非常高级的用法，通过配置文件和启动参数来启动chubaodb。在学这个之前我们可能需要了简单解下chubaodb的架构，chubaodb份了三个角色`master`,`pserver`,`router`.

* master 是集群管理，元数据管理的模块。表结构，数据均衡，failover 等
* pserver 是数据存储及计算节点。
* router 是提供了restful的数据查询组件。router 是无状态的。

下面是一个简单的config例子。也是默认config的参数

````
[global]
# the name will validate join cluster by same name
name = "chubaodb"
# your server ip for connect, you can use -i in rags to set it
# ip = "127.0.0.1"
# log path , If you are in a production environment, You'd better set absolute paths
log = "logs/"
# default log type for any model
log_level = "info"
# log file size for rolling
log_limit_bytes = 128000000
# number of reserved log files
log_file_count = 10
# Whether to use distributed storage. If it's true, there's only one
shared_disk = false

[router]
# port for server
http_port = 8080

[ps]
#set zone num, default is 0
zone = "default"
# you data save to disk path ,If you are in a production environment, You'd better set absolute paths
data = "data/"
# port for server
rpc_port = 9090
# how often to refresh the index
flush_sleep_sec = 3
    [ps.raft]
        heartbeat_port = 10030
        replicate_port = 10031
        # how size of num for memory
        log_max_num = 200000
        # how size of num for memory
        log_min_num = 100000
        # how size of num for memory
        log_file_size_mb = 128
        # Three  without a heartbeat , follower to begin consecutive elections
        heartbeate_ms = 500
    

[[masters]]
# master ip for service
ip = "127.0.0.1"
# port for server
http_port = 7070
# master data path for meta
data = "data/meta/"
````

可以发现我们把 三个模块写到了同一个配置文件。同时各个节点通过参数来选择启动的模块。实现了一个配置文件走天下的易用功能。

可以通过 `./chubaodb --help` 获得参数说明

````
(base) ➜  release git:(async-std) ✗ ./chubaodb --help
chubaodb 0.1.0
hello index world

USAGE:
    chubaodb [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    all       
    help      Prints this message or the help of the given subcommand(s)
    master    
    ps        
    router   
````

启动可以通过 `./chubaodb all --help ` , `./chubaodb ps --help ` .....来获取更多。参数说明

* 比如我们想把三个模块在一个进程中启动 那么就是. `./chubaodb all -c ../../config/config.toml` .

````
./chubaodb all -c ../../config/config.toml
load config by path: ../../config/config.toml
2020-07-15 11:05:39 - INFO - chubaodb::util::config(189) - log init ok 
2020-07-15 11:05:39 - INFO - chubaodb(149) - All ChubaoDB servers were started successfully!
2020-07-15 11:05:39 - INFO - chubaodb::router::server(29) - router is listening on http://0.0.0.0:8080
2020-07-15 11:05:39 - INFO - actix_server::builder(262) - Starting 8 workers
2020-07-15 11:05:39 - INFO - chubaodb::util::http_client(21) - send get for url:http://127.0.0.1:7070/my_ip
2020-07-15 11:05:39 - INFO - surf::middleware::logger::native(119) - sending request
2020-07-15 11:05:39 - INFO - actix_server::builder(276) - Starting "actix-web-service-0.0.0.0:8080" service on 0.0.0.0:8080
2020-07-15 11:05:39 - WARN - isahc::handler(209) - request completed with error [id=AtomicCell { value: 0 }]: ConnectFailed: failed to connect to the server
2020-07-15 11:05:39 - ERROR - chubaodb::pserver::server(41) - got ip from master has err:Error(InternalErr, "ConnectFailed: failed to connect to the server")
2020-07-15 11:05:39 - INFO - chubaodb::master::server(43) - master listening on http://0.0.0.0:7070
2020-07-15 11:05:39 - INFO - actix_server::builder(262) - Starting 8 workers
2020-07-15 11:05:39 - INFO - actix_server::builder(276) - Starting "actix-web-service-0.0.0.0:7070" service on 0.0.0.0:7070
2020-07-15 11:05:40 - INFO - chubaodb::util::http_client(21) - send get for url:http://127.0.0.1:7070/my_ip
2020-07-15 11:05:40 - INFO - surf::middleware::logger::native(119) - sending request
2020-07-15 11:05:40 - INFO - chubaodb::master::server(440) - success_response [Object({"ip": String("127.0.0.1")})]
2020-07-15 11:05:40 - INFO - surf::middleware::logger::native(119) - request completed
2020-07-15 11:05:40 - INFO - chubaodb::pserver::server(36) - got my ip:127.0.0.1 from master
2020-07-15 11:05:40 - INFO - chubaodb::util::http_client(51) - send post for url:http://127.0.0.1:7070/pserver/register
2020-07-15 11:05:40 - INFO - surf::middleware::logger::native(119) - sending request
2020-07-15 11:05:40 - INFO - chubaodb::master::server(301) - prepare to heartbeat with address 127.0.0.1:9090, zone default
2020-07-15 11:05:40 - INFO - chubaodb::master::server(440) - success_response [PServer { id: Some(1), addr: "127.0.0.1:9090", write_partitions: [], zone: "default", modify_time: 0 }]
2020-07-15 11:05:40 - INFO - surf::middleware::logger::native(119) - request completed
2020-07-15 11:05:40 - INFO - chubaodb::pserver::service(111) - register to master ok: node_id:Some(1) 
2020-07-15 11:05:40 - INFO - chubaodb::pserver::service(126) - register server line:PServer { id: Some(1), addr: "127.0.0.1:9090", write_partitions: [], zone: "default", modify_time: 0 }
2020-07-15 11:05:40 - INFO - chubaodb::pserver::server(59) - init pserver OK use time:Ok(5.333ms)
````

* 比如我们只启动 router 那么就是. `./chubaodb router -c ../../config/config.toml` .

````
(base) ➜  release git:(async-std) ✗ ./chubaodb router -c ../../config/config.toml
load config by path: ../../config/config.toml
2020-07-15 11:01:59 - INFO - chubaodb::util::config(189) - log init ok 
2020-07-15 11:01:59 - INFO - chubaodb(149) - All ChubaoDB servers were started successfully!
2020-07-15 11:01:59 - INFO - chubaodb::router::server(29) - router is listening on http://0.0.0.0:8080
2020-07-15 11:01:59 - INFO - actix_server::builder(262) - Starting 8 workers
2020-07-15 11:01:59 - INFO - actix_server::builder(276) - Starting "actix-web-service-0.0.0.0:8080" service on 0.0.0.0:8080
````

* 比如我们只启动 master 那么就是. `./chubaodb master -c ../../config/config.toml` .

````
load config by path: ../../config/config.toml
2020-07-15 11:03:11 - INFO - chubaodb::util::config(189) - log init ok 
2020-07-15 11:03:11 - INFO - chubaodb(149) - All ChubaoDB servers were started successfully!
2020-07-15 11:03:11 - INFO - chubaodb::master::server(43) - master listening on http://0.0.0.0:7070
2020-07-15 11:03:11 - INFO - actix_server::builder(262) - Starting 8 workers
2020-07-15 11:03:11 - INFO - actix_server::builder(276) - Starting "actix-web-service-0.0.0.0:7070" service on 0.0.0.0:7070
^C2020-07-15 11:03:12 - INFO - actix_server::builder(321) - SIGINT received, exiting
````


* 比如我们只启动 pserver 那么就是. `./chubaodb ps -c ../../config/config.toml` . ps会通过你配置文件中master的配置自动去master 注册为数据节点

````
load config by path: ../../config/config.toml
2020-07-15 11:03:45 - INFO - chubaodb::util::config(189) - log init ok 
2020-07-15 11:03:45 - INFO - chubaodb(149) - All ChubaoDB servers were started successfully!
2020-07-15 11:03:45 - INFO - chubaodb::util::http_client(21) - send get for url:http://127.0.0.1:7070/my_ip
2020-07-15 11:03:45 - INFO - surf::middleware::logger::native(119) - sending request
2020-07-15 11:03:45 - WARN - isahc::handler(209) - request completed with error [id=AtomicCell { value: 0 }]: ConnectFailed: failed to connect to the server
````

