# Load testing

We included a sample load testing client in the [../examples/load-tester](../examples/load-tester/) directory. In order to run the test, run the following command:

```
go run examples/load-tester/main.go -address=<address of the Open Saves server> -connections <number of concurrent connections> \
    -concurrency <number of concurrent requests per connection> -requests <number of requests> \
    -tester <name of the load tester>
```

The following command, for example, invokes the Ping method on a local server using insecure connections (e.g. without TLS).

```
go run examples/load-tester/main.go -address=localhost:6000 -connections 32 \
    -concurrency 32 -requests 100000 \
    -tester ping -insecure
```

The output should look like this.

```
INFO[0000] Starting Ping calls, connections = 32, concurrency = 32, requests = 100000 
195648.10062287704 pings/second (99328 calls in 0.507687014 seconds)
```

This shows the program performed 99,328 ping calls in 0.50 seconds. Please note the exact number of calls might not match with the number of requests specified.

## Available test methods

The following test methods are available.

### ping

Calls the Ping RPC method.

### getrecord

Calls the GetRecord RPC method to the single Record.

## Connections and concurrency

The GRPC library supports both multiple connections and method call multiplexing over a single connection. The -concurrency option specifies the number of goroutines per connection, and the -connections option specifies the number of connections that the tester uses. For example, `-connections 32 -concurrency 32` uses 32 connections and 32 goroutines per connection, 1024 goroutines in total.

