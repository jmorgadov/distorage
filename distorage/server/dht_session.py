"""
DHT session service for node communications.
"""

from __future__ import annotations

import sys
import typing
from typing import Union

import rpyc

from distorage.server import config
from distorage.server.logger import logger
from distorage.server.server_manager import ServerManager

if typing.TYPE_CHECKING:
    from distorage.server.dht import ChordNode, DhtID


def ensure_registered(func):
    """Ensures that the dht node is resgistered before doing anything."""

    def wrapper(self: DhtSessionService, *args, **kwargs):
        if self.dht_id != -1:
            return False, "Not registered"
        return func(self, *args, **kwargs)

    return wrapper


class DhtSessionService(rpyc.Service):
    """
    The RPyC service for the DHT session.
    """

    def __init__(self):
        self.dht_id = -1
        self.dht_node: Union[ChordNode, None] = None

    @property
    def node(self) -> ChordNode:
        """DHT node."""
        assert self.dht_node is not None
        return self.dht_node

    def exposed_register(self, dht_id: DhtID, passwd: str):
        """Register the Dht node"""
        self.dht_id = dht_id
        if passwd != ServerManager.passwd:
            return False, "Wrong password"
        return True, ""

    @ensure_registered
    def exposed_join(self, node_ip: str):
        """
        Join the DHT node to the network.
        """
        return self.node.join(node_ip)

    @ensure_registered
    def exposed_find_successor(self, node_id: int) -> str:
        """
        Find the successor node of a specific id.

        Parameters
        ----------
        node_id : str
            The key to find it's successor in a CHORD ring.
        dht_id : DhtID
            The DHT ID of the node.
        """
        return self.node.find_successor(int(node_id))

    def exposed_get_predecessor(self) -> Union[str, None]:
        """
        Get the predecessor of the DHT node.
        """
        return self.node.predecessor

    def exposed_notify(self, node_ip: str):
        """
        Notify the DHT node of a possible predecessor.
        """
        self.node.notify(node_ip)


class DhtSession:
    """DHT session context manager."""

    def __init__(self, server_ip: str, dht_id: DhtID):
        self.dht_id = dht_id
        self.server_ip = server_ip
        self.dht_session: Union[rpyc.Connection, None] = None

    def __enter__(self):
        self.dht_session = rpyc.connect(self.server_ip, port=config.DHT_PORT)
        succ, msg = self.dht_session.root.register(self.dht_id, ServerManager.passwd)
        if not succ:
            logger.error(msg)
            sys.exit(1)
        return self.dht_session.root

    def __exit__(self, exc_type, exc_value, traceback):
        assert self.dht_session is not None
        self.dht_session.close()
