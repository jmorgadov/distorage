"""
This contains the server session service used for inter-servers communication.
"""
from __future__ import annotations

import sys
from datetime import datetime
from typing import List, Tuple, Union

import rpyc

from distorage.server import config
from distorage.server.logger import logger
from distorage.server.server_manager import ServerManager


def ensure_registered(func):
    """Ensures that the server is resgistered before doing anything."""

    def wrapper(self: ServerSessionService, *args, **kwargs):
        if self.server_ip not in ServerManager.knwon_servers:
            return False, "Not registered"
        return func(self, *args, **kwargs)

    return wrapper


class ServerSessionService(rpyc.Service):
    """Server session service."""

    def __init__(self):
        self.server_ip: str = ""

    def exposed_register(self, server_ip: str, passwd: str) -> Tuple[bool, str]:
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
        Tuple[bool, str]
            The success of the operation and the message.
        """
        if server_ip in ServerManager.knwon_servers:
            self.server_ip = server_ip
            return True, "Server already registered"

        if passwd != ServerManager.passwd:
            logger.info("Server %s try to connect using a password", server_ip)
            return False, "Wrong password"

        logger.info("Registering server %s", server_ip)
        self.server_ip = server_ip
        ServerManager.add_server(server_ip)
        return True, "Server registered successfully"

    @ensure_registered
    def exposed_get_known_servers(self) -> List[str]:  # pylint: disable=no-self-use
        """
        Gets the list of knowns servers.

        Returns
        -------
        List[str]
            The list of known servers IP addreses.
        """
        return list(ServerManager.knwon_servers.keys())


class ServerSession:
    """Server session context manager."""

    def __init__(self, server_ip: str, passwd: str):
        self.server_ip = server_ip
        self.passwd = passwd
        self.server_session: Union[rpyc.Connection, None] = None

    def __enter__(self):
        self.server_session = rpyc.connect(self.server_ip, port=config.SERVER_PORT)
        succ, msg = self.server_session.root.register(
            ServerManager.host_ip, self.passwd
        )
        if not succ:
            logger.error(msg)
            sys.exit(1)
        server_info = ServerManager.knwon_servers[self.server_ip]
        server_info["last_active"] = datetime.now()
        return self.server_session.root

    def __exit__(self, exc_type, exc_value, traceback):
        assert self.server_session is not None
        self.server_session.close()
