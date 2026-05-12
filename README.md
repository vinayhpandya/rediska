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