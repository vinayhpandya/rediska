# learning-redis
This repository is for writing down all the essential details about redis and how it will be implemented in go or any other language

## Milestone 1
Build a simple tcp echo server which serves echoes the command back to the user
Running the server
```bash
  go run . --host 127.0.0.1 --port 7973
```

Verifying the echo command
```bash
nc 127.0.0.1 7379
HELLO
```

## Milestone 2 
Build a simple RESP parser which can understand the commands coming from redis cli

From the client run the redis cli and connect it to the port where the server is
running
```bash
redis-cli -p 7379 set k v
```
On the server we see that the commands are read and parsed
```bash
go run . --host 127.0.0.1 --port 7379
Starting Rediska on host 127.0.0.1 and port 7379 
rediska is listening on 127.0.0.1:7379 
2026/05/14 11:37:59 client connected: 127.0.0.1:51367
2026/05/14 11:37:59 received from 127.0.0.1:51367: []interface {}{"ping"}
2026/05/14 11:37:59 client disconnected: 127.0.0.1:51367 
2026/05/14 11:38:26 client connected: 127.0.0.1:51416
2026/05/14 11:38:26 received from 127.0.0.1:51416: []interface {}{"set", "k", "v"}
2026/05/14 11:38:26 client disconnected: 127.0.0.1:51416 
```