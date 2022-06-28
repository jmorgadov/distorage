"""
This contains the server session service used for inter-servers communication.
"""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Union

from distorage.logger import logger
from distorage.server import config
from distorage.server.dht.dht_id_enum import DhtID

if TYPE_CHECKING:
    from distorage.server.dht.dht import ChordNode


class ServerManager:
    """Manages the server sessions."""

    knwon_servers: Dict[str, Any] = {}
    host_ip: str = ""
    passwd: str = ""
    server_started: bool = False

    _clients_dht: Union[ChordNode, None] = None
    _data_dht: Union[ChordNode, None] = None

    @staticmethod
    def get_dht(dht_id: DhtID) -> ChordNode:
        """
        Get the DHT node for the given DHT ID.
        """
        if dht_id == DhtID.CLIENT:
            return ServerManager.clients_dht()
        if dht_id == DhtID.DATA:
            return ServerManager.data_dht()
        raise ValueError(f"Invalid DHT ID: {dht_id}")

    @staticmethod
    def clients_dht() -> ChordNode:
        """
        Get the clients DHT node.
        """
        assert ServerManager._clients_dht is not None
        return ServerManager._clients_dht

    @staticmethod
    def data_dht() -> ChordNode:
        """
        Get the data DHT node.
        """
        assert ServerManager._data_dht is not None
        return ServerManager._data_dht

    @staticmethod
    def setup(host_ip: str, passwd: str, clients_dht: ChordNode, data_dht: ChordNode):
        """
        Initializes the server session manager.

        Parameters
        ----------
        host_ip : str
            The IP address of the host server.
        passwd : str
            The password of the server.
        """
        ServerManager.host_ip = host_ip
        ServerManager.passwd = passwd
        ServerManager._clients_dht = clients_dht
        ServerManager._data_dht = data_dht

    @staticmethod
    def add_server(server_ip: str):
        """
        Adds a server to the list of allowed servers.

        Parameters
        ----------
        server_ip : str
            The IP address of the server.
        """
        if (
            server_ip in ServerManager.knwon_servers
            or server_ip == ServerManager.host_ip
        ):
            return

        logger.info("Adding server %s", server_ip)
        ServerManager.knwon_servers[server_ip] = {
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
        if server_ip not in ServerManager.knwon_servers:
            return False

        info = ServerManager.knwon_servers[server_ip]
        last_active: datetime = info["last_active"]
        if last_active is None:
            return False

        now = datetime.now()
        if (now - last_active).total_seconds() > config.DISCOVER_TIMEOUT:
            logger.info("Server %s is no longer active", server_ip)
            ServerManager.knwon_servers.pop(server_ip)
            return True
        return False
