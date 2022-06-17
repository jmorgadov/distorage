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
from typing import Union

import rpyc
from rpyc.utils.server import ThreadedServer

from distorage.server import config
from distorage.server.client_session import ClientSessionService
from distorage.server.logger import logger
from distorage.server.server_manager import ServerManager
from distorage.server.server_session import ServerSession, ServerSessionService


async def server_started():
    while not ServerManager.server_started:
        await asyncio.sleep(0.2)


def _start_client_sessions():
    client_sessions = ThreadedServer(
        ClientSessionService, hostname=ServerManager.host_ip, port=config.CLIENT_PORT
    )
    logger.info(
        "Client sessions listener started on %s:%s",
        ServerManager.host_ip,
        config.CLIENT_PORT,
    )
    ServerManager.server_started = True
    client_sessions.start()


async def start_client_sessions_listener():
    await server_started()
    threading.Thread(target=_start_client_sessions).start()


def _start_host_server(passwd: str):
    host_ip = socket.gethostbyname(socket.gethostname())
    port = config.SERVER_PORT
    ServerManager.setup(host_ip, passwd)
    server = ThreadedServer(ServerSessionService, hostname=host_ip, port=port)
    logger.info("Server sessions listener started on %s:%s", host_ip, port)
    ServerManager.server_started = True
    server.start()


def start_host_server(passwd: str):
    threading.Thread(target=_start_host_server, args=(passwd,)).start()
    asyncio.run(start_client_sessions_listener())


async def discover_servers_routine():
    """Discovers the servers in the network."""
    await server_started()
    while True:
        logger.debug("Discovering servers...")
        known_servers = list(ServerManager.knwon_servers.keys())
        for known_ip in known_servers:
            # Check if the server hasn't been active for a while
            if ServerManager.check_server_timeout(known_ip):
                continue

            # Discover new servers
            try:
                with ServerSession(known_ip, ServerManager.passwd) as session:
                    known_servers = session.get_known_servers()
                    logger.debug(f"Known servers: {known_servers}")
                    for discovered_ip in known_servers:
                        ServerManager.add_server(discovered_ip)
            except:
                logger.debug("Failed to connect to %s for discovering", known_ip)

        await asyncio.sleep(config.DISOCVER_INTERVAL)


def ask_passwd() -> str:
    passwd = getpass("Enter the password for the new system: \n> ").strip()
    passwd_conf = getpass("Repeat the password for confirmation: \n> ").strip()
    if passwd != passwd_conf:
        logger.error("Passwords do not match!")
        sys.exit(1)
    return passwd


async def run_coroutines():
    await server_started()
    asyncio.run(discover_servers_routine())


def setup_new_system(passwd: Union[str, None] = None):
    """Setups a new system."""
    if not passwd:
        passwd = ask_passwd()
    start_host_server(passwd)
    logger.info("New system setup successfully!")
    asyncio.run(run_coroutines())


def connect_to_system(server_ip: str, passwd: Union[str, None] = None):
    """Connects to the system."""
    if not passwd:
        passwd = ask_passwd()
    start_host_server(passwd)
    ServerManager.add_server(server_ip)
    asyncio.run(run_coroutines())


def search_local_servers() -> Union[str, None]:
    """Searches for local servers."""
    host_ip = socket.gethostbyname(socket.gethostname())
    subnet = ".".join(host_ip.split(".")[:-1]) + "."
    port = config.SERVER_PORT
    for i in range(1, 254):
        server_ip = f"{subnet}{i}"
        try:
            conn = rpyc.connect(server_ip, port=port)
            conn.close()
            logger.info("Found server at %s", server_ip)
            return server_ip
        except:
            continue
    logger.info("No local servers found")
    return None


def main():
    if len(sys.argv) < 2:
        print("No command given. Options are: new, connect, discover")
        sys.exit(1)

    command = sys.argv[1]
    server_ip = ""

    # Setup a new system
    if command == "new":
        passwd = None if len(sys.argv) < 3 else sys.argv[2]
        setup_new_system(passwd)

    # Connect to a system
    elif command == "connect":
        if len(sys.argv) < 3:
            print("No server IP given")
            sys.exit(1)
        server_ip = sys.argv[2]
        if server_ip and not re.match(
            r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", server_ip
        ):
            print(f"Invalid IP address: '{server_ip}'")
            sys.exit(1)
        passwd = None if len(sys.argv) < 4 else sys.argv[3]
        connect_to_system(server_ip, passwd)

    # Discover local servers
    elif command == "discover":
        passwd = None if len(sys.argv) < 3 else sys.argv[2]
        server_ip = search_local_servers()
        if server_ip:
            logger.info("Connecting to system at %s", server_ip)
            connect_to_system(server_ip, passwd)
        else:
            logger.error("No local servers found")
            sys.exit(1)
    else:
        print(f"Invalid command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
