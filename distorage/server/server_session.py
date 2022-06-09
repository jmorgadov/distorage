"""
This contains the server session service used for inter-servers communication.
"""
from __future__ import annotations

import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, TypeVar

import rpyc

from distorage.server import config
from distorage.server.logger import logger

T = TypeVar("T")
ServerResponse = Tuple[bool, T]


class ServerSessionManager:
    """Manages the server sessions."""

    knwon_servers: Dict[str, Any] = {}
    host_ip: str = ""
    passwd: str = ""
    server_started: bool = False

    @staticmethod
    def setup(host_ip: str, passwd: str):
        """
        Initializes the server session manager.

        Parameters
        ----------
        host_ip : str
            The IP address of the host server.
        passwd : str
            The password of the server.
        """
        ServerSessionManager.host_ip = host_ip
        ServerSessionManager.passwd = passwd

    @staticmethod
    def add_server(server_ip: str):
        """
        Adds a server to the list of allowed servers.

        Parameters
        ----------
        server_ip : str
            The IP address of the server.
        """
        if server_ip in ServerSessionManager.knwon_servers:
            return

        logger.info(f"Adding server {server_ip}")
        ServerSessionManager.knwon_servers[server_ip] = {
            "last_active": None,
        }

    @staticmethod
    def check_server_timeout(server_ip: str) -> bool:
        """
        Checks if too much time has passed since the last communication
        with the server.

        If so, the server is removed from the list of known servers.

        Parameters
        ----------
        server_ip : str
            The IP address of the server.

        Returns
        -------
        bool
            True if the server is removed, False otherwise.
        """
        if server_ip not in ServerSessionManager.knwon_servers:
            return False

        info = ServerSessionManager.knwon_servers[server_ip]
        last_active: datetime = info["last_active"]
        if last_active is None:
            return False

        now = datetime.now()
        if (now - last_active).total_seconds() > config.DISCOVER_TIMEOUT:
            logger.info("Server %s is no longer active", server_ip)
            ServerSessionManager.knwon_servers.pop(server_ip)
            return True
        return False


def ensure_registered(func):
    """Ensures that the server is resgistered before doing anything."""

    def wrapper(self: ServerSessionService, *args, **kwargs):
        if self.server_ip not in ServerSessionManager.knwon_servers:
            return False, "Not registered"
        return func(self, *args, **kwargs)

    return wrapper


class ServerSessionService(rpyc.Service):
    """Server session service."""

    def __init__(self):
        self.server_ip: str = ""

    def exposed_register(self, server_ip: str, passwd: str) -> ServerResponse[str]:
        """
        Registers the server.

        Parameters
        ----------
        server_ip : str
            The IP address of the server.
        passw : str
            The password of the server.

        Returns
        -------
        ServerResponse[str]
            Message of the response.
        """
        if server_ip in ServerSessionManager.knwon_servers:
            self.server_ip = server_ip
            return True, "Server already registered"

        if passwd != ServerSessionManager.passwd:
            logger.info("Incorrect server password")
            return False, "Wrong password"

        logger.info(f"Registering server '{server_ip}'")
        self.server_ip = server_ip
        ServerSessionManager.add_server(server_ip)
        return True, "Server registered successfully"

    @ensure_registered
    def exposed_get_known_servers(self) -> List[str]:
        """
        Gets the list of knowns servers.

        Returns
        -------
        ServerResponse[List[str]]
            The list of known servers IP addreses.
        """
        return list(ServerSessionManager.knwon_servers.keys())


class ServerSession:
    """Server session context manager."""

    def __init__(self, server_ip: str, passwd: str):
        self.server_ip = server_ip
        self.passwd = passwd
        self.server_session: Optional[rpyc.Connection] = None

    def __enter__(self):
        self.server_session = rpyc.connect(self.server_ip, port=config.SERVER_PORT)
        ret, msg = self.server_session.root.register(
            ServerSessionManager.host_ip, self.passwd
        )
        if not ret:
            logger.error(msg)
            sys.exit(1)
        server_info = ServerSessionManager.knwon_servers[self.server_ip]
        server_info["last_active"] = datetime.now()
        return self.server_session.root

    def __exit__(self, exc_type, exc_value, traceback):
        assert self.server_session is not None
        self.server_session.close()
