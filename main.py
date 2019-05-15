import sys
import asyncio
from raft import Raft
from protocol import UDPProtocol, TCPProtocol

def main():
    config = {"address": ("127.0.0.1", int(sys.argv[1])),
     "cluster": [("10.12.43.212", 6001), ("10.12.231.81", 6002), ("10.13.61.65", 6003)]}

    loop = asyncio.get_event_loop()
    raft = Raft(config=config, port=int(sys.argv[1]))
    udp = loop.create_datagram_endpoint(lambda: UDPProtocol(loop, raft), local_addr=('127.0.0.1', int(sys.argv[1])))
    loop.run_until_complete(udp)
    tcp = loop.create_server(lambda: TCPProtocol(loop, raft), '127.0.0.1', int(sys.argv[1]))
    loop.run_until_complete(tcp)
    loop.run_forever()

if __name__ == "__main__":
    main()
