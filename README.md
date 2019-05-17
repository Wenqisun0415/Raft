# Simple Key-Value Pair Database

This is a distributed key-value pair database system which is a naive version of the Nosql redis based on the raft algorithm.

Different user can have access to the same database therefore allowing data to be shared.

## Structure of the package

There are seven python files in total.

* `main.py`: The file to start the server
* `client.py`: The file to start the client
* `log.py`: This file defines the log class itself as well as the operations that can be performed on it
* `persist.py`: This file defines the persist class which includes the log object as well as other variables which need to be stored persistently.
* `protocol.py`: This file includes a TCP protocol used for communication between client and server and a UDP protocol used by servers to communicate with each other
* `state.py`: The detail logic of servers in different states: Follower, Candidate and Leader
* `raft.py`: This class is the skeleton of the raft consensus algorithm, and all attributes of the server are defined here

## How to use

### Server

In several terminals, run the following command to start the server cluster:

```
$ python3 main.py 6001
```

### Client

Open a new terminal, run the following command:

```
$ python3 -i client.py
>>> client = Client()
>>> c.get("list")
(nil)
>>> c.insert("list", [1,2,3])
OK
>>> c.get("list")
[1,2,3]
>>> c.delete("list")
(integer) 1
```


