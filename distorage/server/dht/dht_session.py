"""
DHT session service for node communications.
"""

from __future__ import annotations

import typing
from typing import Any, Union

import rpyc

from distorage.exceptions import ServiceConnectionError
from distorage.logger import logger
from distorage.response import Response, VoidResponse, new_error_response
from distorage.server import config
from distorage.server.dht.dht_id_enum import DhtID
from distorage.server.server_manager import ServerManager

if typing.TYPE_CHECKING:
    from distorage.server.dht.dht import ChordNode


def _ensure_registered(func):
    """Ensures that the dht node is resgistered before doing anything."""

    def wrapper(self: DhtSessionService, *args, **kwargs):
        if self.dht_id == -1:
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

    def exposed_register(self, dht_id: int, passwd: str) -> VoidResponse:
        """Register the Dht node"""
        self.dht_id = DhtID(dht_id)
        self.dht_node = ServerManager.get_dht(self.dht_id)
        if passwd != ServerManager.passwd:
            return new_error_response("Wrong password")
        return new_error_response("Registered")

    @_ensure_registered
    def exposed_join(self, node_ip: str) -> Response[str]:
        """Join the DHT node to the network."""
        return self.node.join(node_ip)

    @_ensure_registered
    def exposed_find_successor(self, node_id: int) -> Response[str]:
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

    @_ensure_registered
    def exposed_get_predecessor(self) -> Union[str, None]:
        """Get the predecessor of the DHT node."""
        return self.node.predecessor

    @_ensure_registered
    def exposed_notify(self, node_ip: str):
        """Notify the DHT node of a possible predecessor."""
        self.node.notify(node_ip)

    @_ensure_registered
    def exposed_find(self, elem_key: str, is_file: bool = False) -> Response[Any]:
        """Find an element in the DHT."""
        return self.node.find(elem_key, is_file)

    @_ensure_registered
    def exposed_store(  # pylint: disable=too-many-arguments
        self,
        elem_key: str,
        elem: Any,
        overwrite: bool = True,
        check_removed: bool = False,
        persist_path: Union[str, None] = None,
    ) -> VoidResponse:
        """Store an element in the DHT."""
        return self.node.store(elem_key, elem, overwrite, check_removed, persist_path)

    @_ensure_registered
    def exposed_store_replica(
        self, elem_key: str, elem: Any, persist_path: Union[str, None] = None
    ) -> VoidResponse:
        """Store a replica of an element in the node."""
        return self.node.store_replica(elem_key, elem, persist_path)

    @_ensure_registered
    def exposed_remove(self, elem_key: str) -> VoidResponse:
        """Remove an element from the DHT."""
        return self.node.remove(elem_key)

    @_ensure_registered
    def exposed_remove_replica(self, elem_key: str) -> VoidResponse:
        """Remove a replica of an element from the DHT."""
        return self.node.remove_replica(elem_key)


class DhtSession:
    """DHT session context manager."""

    def __init__(self, server_ip: str, dht_id: DhtID):
        self.dht_id = dht_id
        self.server_ip = server_ip
        self.dht_session: Union[rpyc.Connection, None] = None

    def __enter__(self):
        try:
            self.dht_session = rpyc.connect(self.server_ip, port=config.DHT_PORT)
        except Exception as exc:
            logger.error("Could not connect to DHT server")
            raise ServiceConnectionError("Could not connect to DHT server") from exc

        resp: VoidResponse = self.dht_session.root.register(
            self.dht_id, ServerManager.passwd
        )
        if not resp:
            logger.error(resp.msg)
            raise ServiceConnectionError(resp.msg)
        return self.dht_session.root

    def __exit__(self, exc_type, exc_value, traceback):
        assert self.dht_session is not None
        self.dht_session.close()
