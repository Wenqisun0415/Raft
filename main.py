import sys
import asyncio
from raft import Raft
from protocol import UDPProtocol

def main():
    config = {"address": ("127.0.0.1", int(sys.argv[1])),
     "cluster": [("127.0.0.1", 6001), ("127.0.0.1", 6002), ("127.0.0.1", 6003)]}

    loop = asyncio.get_event_loop()
    raft = Raft(config=config, port=int(sys.argv[1]))
    connect = loop.create_datagram_endpoint(lambda: UDPProtocol(loop, raft), local_addr=('127.0.0.1', int(sys.argv[1])))
    loop.run_until_complete(connect)
    loop.run_forever()

if __name__ == "__main__":
    main()
