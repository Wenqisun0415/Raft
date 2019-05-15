import asyncio
import logging
import logging.config
import json

logging.config.fileConfig(fname='file.conf', disable_existing_loggers=False)
logger = logging.getLogger("raft")

class UDPProtocol:

    def __init__(self, loop, raft):
        self.loop = loop
        self.raft = raft
        logger.info("UDP start")

    def connection_made(self, transport):
        self.transport = transport
        self.raft.transport = transport
    
    def datagram_received(self, data, addr):
        self.raft.receive_peer_message(addr, json.loads(data.decode()))

    def error_received(self, exc):
        logger.error(f"Error occurred: {exc}")

class TCPProtocol(asyncio.Protocol):

    def __init__(self, loop, raft):
        self.loop = loop
        self.raft = raft
        logger.info("TCP start")

    def connection_made(self, transport):
        self.transport = transport
        logger.info("Client request received")

    def data_received(self, data):
        self.raft.receive_client_message(json.loads(data.decode()), self.transport)
