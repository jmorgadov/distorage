# This is just to declare some basic needed actions
# that a server will do. This might be inplemented
# in other modules in the future.
"""
This will contain all the server related implementation of the system.
"""

import asyncio
import re
import socket
import sys
import threading
from getpass import getpass

import rpyc
from rpyc.utils.server import ThreadedServer

from distorage.server import config
from distorage.server.logger import logger
from distorage.server.server_session import (
    ServerSession,
    ServerSessionManager,
    ServerSessionService,
)


def _start_host_server(passwd: str):
    host_ip = socket.gethostbyname(socket.gethostname())
    port = config.SERVER_PORT
    ServerSessionManager.setup(host_ip, passwd)
    server = ThreadedServer(ServerSessionService, hostname=host_ip, port=port)
    logger.info(f"Server session started on %s:%s", host_ip, port)
    ServerSessionManager.server_started = True
    server.start()


async def server_started():
    while not ServerSessionManager.server_started:
        await asyncio.sleep(0.2)


async def discover_servers_routine():
    """Discovers the servers in the network."""
    await server_started()
    while True:
        logger.debug("Discovering servers...")
        known_servers = list(ServerSessionManager.knwon_servers.keys())
        for known_ip in known_servers:
            # Check if the server hasn't been active for a while
            if ServerSessionManager.check_server_timeout(known_ip):
                continue

            # Discover new servers
            try:
                with ServerSession(known_ip, ServerSessionManager.passwd) as server:
                    known_servers = server.get_known_servers()
                    logger.debug(f"Known servers: {known_servers}")
                    for discovered_ip in known_servers:
                        ServerSessionManager.add_server(discovered_ip)
            except Exception as e:
                print(type(e))
                logger.debug("Failed to connect to %s for discovering", known_ip)

        await asyncio.sleep(config.DISOCVER_INTERVAL)


def start_host_server(passwd: str):
    threading.Thread(target=_start_host_server, args=(passwd,)).start()


def setup_new_system():
    """Setups a new system."""
    passwd = getpass("Enter the password for the new system: \n> ").strip()
    passwd_conf = getpass("Repeat the password for confirmation: \n> ").strip()
    if passwd != passwd_conf:
        logger.error("Passwords do not match!")
        sys.exit(1)
    logger.info("New system setup successfully!")
    start_host_server(passwd)
    asyncio.run(discover_servers_routine())


def connect_to_system(ip: str, passwd: str):
    """Connects to the system."""
    start_host_server(passwd)
    ServerSessionManager.add_server(ip)
    asyncio.run(discover_servers_routine())


def main():
    if len(sys.argv) > 1:
        server_ip = sys.argv[1]
    else:
        server_ip = input(
            "Enter the IP address of one of the system servers.\n"
            "Leave it blank to start a new system.\n"
            "> ",
        ).strip()

    if server_ip and not re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", server_ip):
        raise ValueError(f"Invalid IP address: '{server_ip}'")

    if not server_ip:
        setup_new_system()
        return

    logger.info("Connecting to system at %s", server_ip)
    passwd = getpass("Enter the password: \n> ").strip()
    connect_to_system(server_ip, passwd)


if __name__ == "__main__":
    main()
