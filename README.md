# Octavius - Fully Decentralized Distributed File System and Stream Processing System

Octavius is a distributed file system and stream processing system. It uses elements of consistent hashing to 
enable a fully-decentralized system and has no single point of failure. 

Octavius is more resilient and significantly faster than the Apache Hadoop File System, and is faster than 
Apache Spark Streaming for Stream Processing.

# Build

- Download common dependencies as ```go get -u github.com/rahulgovind/octavius```
- Download server dependencies as ```go get -u github.com/rahulgovind/octavius/server```
- Build Server Code ```go build -o DEST/server github.com/rahulgovind/octavius/server```
- Build Client Code ```go build -o DEST/client github.com/rahulgovind/octavius/examples/arithmetic```
- Create config file config.json with addresses of master nodes. For example, 
``["127.0.0.1:8000", "127.0.0.1:8001"]``

# Usage

The coordinators have to be started before running anything else. Start the coordinators as
```
PATH_TO_EXECUTABLE/server --coordinator [--ip IP] [--port port]
```
After this, any node can be started as
```
PATH_TO_EXECUTABLE/server [--ip IP] [--port port]
```
The IP and port the node should listen on need to be specified too. Refer to other details by running
```
PATH_TO_EXECUTABLE/server -h
```

We currently test code on the local machine by first building the server code.
We then run `<path-to-server-executable> --coordinator --port <port>` and `<path-to-server-executable --port <port>`. 
The default parameters are meant for local execution and we recommend you follow 
this method if you wish to quickly test our code.


## SDFS 
- Put: `put <local file> <sdfs file>`
- Get: `get <sdfs file> <local file>`
- Get Versions: `get-versions <sdfs file> <num-versions> <local-file>`
- Delete: `delete <sdfs file>`

# SDFS-Octavius
- Put data: `putjson <local file> <sdfs file>`