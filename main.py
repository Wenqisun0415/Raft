import sys
import asyncio
from raft import Raft
from protocol import UDPProtocol, TCPProtocol
import socket

# Main function to start or test
def main():

    hostname = socket.gethostname()    
    IPAddr = socket.gethostbyname(hostname)

    config = {"address": (IPAddr, int(sys.argv[1])),
     "cluster": [("10.12.43.212", 6001), ("10.12.231.81", 6001), ("10.13.61.65", 6001)]}

    loop = asyncio.get_event_loop()
    raft = Raft(config=config, port=int(sys.argv[1]))
    udp = loop.create_datagram_endpoint(lambda: UDPProtocol(loop, raft), local_addr=(IPAddr, int(sys.argv[1])))
    loop.run_until_complete(udp)
    tcp = loop.create_server(lambda: TCPProtocol(loop, raft), IPAddr, int(sys.argv[1]))
    loop.run_until_complete(tcp)
    loop.run_forever()

if __name__ == "__main__":
    main()
