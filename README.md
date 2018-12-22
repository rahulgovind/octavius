# Octavius - Fully Decentralized Distributed File System and Stream Processing System

Octavius is a distributed file system and stream processing system. It uses elements of consistent hashing to 
enable a fully-decentralized system and has no single point of failure. 

Octavius is more resilient and significantly faster than the Apache Hadoop File System, and is faster than 
Apache Spark Streaming for Stream Processing.

# Features
- SWIM-based Failure Detector for O(1) time for failure detection,  O(log N) for dissemination and O(1) load per 
server
- Efficient Active replication of files in Distributed File System. Fully decentralized.
- Fault-tolerant stream processing. Fully decentralized.

# Build

- Download common dependencies as ```go get -u github.com/rahulgovind/octavius```
- Download server dependencies as ```go get -u github.com/rahulgovind/octavius/server```
- Build Server Code ```go build -o PATH_TO_EXECUTABLE/server github.com/rahulgovind/octavius/server```
- Build Client Code ```go build -o PATH_TO_EXECUTABLE/client github.com/rahulgovind/octavius/examples/arithmetic```
- Create config file config.json with addresses of master nodes. For example, 
``["127.0.0.1:8000", "127.0.0.1:8001"]``. You can simply copy the 

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


## Running octavius and the example code out of the box locally
```
PATH_TO_EXECUTABLE/server --coordinator --port 4001
PATH_TO_EXECUTABLE/server --coordinator --port 4002
PATH_TO_EXECUTALBE/server --port 4010
# Example code
PATH_TO_EXECUTABLE/arithmetic
```

Then you can add files to the stream processor set up by the `arithmetic` example. Consider the file `test.json` below
```
1
2
3
```

Input the following command to the standard input of the server (running on port 4010)
```
putjson test.json Spout
```

This will create the file `Sink` on the distributed file system. You can get this file locally by running 
`get Sink path/to/some/directory/sink`

The file `sink` should have the following contents
```
2
4
6
```

## Usage: Distributed File System (DFS)
You can run the following commands on any of the worker (non-coordinator) servers you start. Note that the 
underlying distributed file system is a versioned file system. So if you upload files with the same name twice, it 
will store copies of both the newer version and the older version.

- Put / Upload file to distributed file system: `put <path to local file> <dfs file name>`
- Get: `get <dfs file name> <path to local file>`
- Get Versions: `get-versions <dfs file name> <num-versions> <path to local file>`
- Delete: `delete <dfs file name>`
- Check files stored at server (Only shows files stored locally): `store`
- Put data for stream processing: `putjson <path to local file> <dfs file name>`
