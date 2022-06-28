"""
Contains a Distribute Hash Table implementation using CHORD protocol.
"""

from __future__ import annotations

import threading
import time
from hashlib import sha1
from typing import Any, Dict, List, Union

import rpyc

from distorage.exceptions import ServiceConnectionError
from distorage.logger import logger
from distorage.response import (
    Response,
    VoidResponse,
    new_error_response,
    new_response,
    new_void_respone,
)
from distorage.server import config
from distorage.server.dht.dht_id_enum import DhtID
from distorage.server.dht.dht_session import DhtSession


def _belongs(value: int, lower: int, upper: int) -> bool:
    """
    Checks whether a value its contained in a range of a circular array of elements

    Parameters
    ----------
    value : int
        The value to check.
    lower : int
        The lower value of the range.
    upper : int
        The upper value of the range.
    """
    return (lower < value <= upper) or (
        lower > upper and (lower < value or value < upper)
    )


def _get_name(elem_key):
    elem_name = str(elem_key)
    if len(elem_name) > 13:
        elem_name = f"{elem_name[:5]}...{elem_name[-5:]}"
    return elem_name


def sha1_hash(value: str) -> int:
    """
    Apply a hash function (SHA-1) to a string value

    Parameters
    ----------
    value : str
        The value to be hashed.
    """
    _hs = sha1(value.encode("utf-8")).digest()
    return int.from_bytes(_hs, byteorder="big")


class ChordNode:
    """
    A Node based on CHORD protocol.
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(self, ip_addr: str, dht_id: DhtID):
        self.ip_addr = ip_addr
        self.dht_id = dht_id
        self.node_id: int = sha1_hash(ip_addr)
        self.predecessor: Union[str, None] = None
        self.successor: str = ip_addr
        self.fingers: List[str] = [""] * 160
        self.next: int = -1
        self.elems: Dict[int, Any] = {}
        self.repl_elems: Dict[int, Any] = {}
        self.run_coroutines()

    def log(self, msg):
        """
        Logs a message.

        Parameters
        ----------
        msg : str
            The message to be logged.
        """
        logger.debug("Node [%s %s]: %s", self.ip_addr, str(self.dht_id), msg)

    def run_coroutines(self):
        """
        Runs all the coroutines of the node.
        """
        threading.Thread(target=self.stabilize).start()
        threading.Thread(target=self.fix_fingers).start()
        threading.Thread(target=self.check_predecessor).start()

    def find_successor(self, node_id: int) -> Response[str]:
        """
        Find the successor node of a specific id.

        Parameters
        ----------
        node_id : int
            The key to find it's successor in a CHORD ring.
        """
        if _belongs(node_id, self.node_id, sha1_hash(self.successor)):
            return new_response(self.successor)

        closest = self.closest_preceding_node(node_id)
        if closest == self.ip_addr:
            return new_response(self.ip_addr)
        try:
            with DhtSession(closest, self.dht_id) as session:
                succ = session.find_successor(node_id)
                return succ
        except ServiceConnectionError:
            return new_response(
                "", success=False, msg=f"Connection error to node {closest}"
            )

    def closest_preceding_node(self, node_id: int) -> str:
        """
        Find the closest preceding node of a specific id.

        Parameters
        ----------
        node_id : int
            The key to find it's successor in a CHORD ring.
        """
        for i in range(len(self.fingers) - 1, -1, -1):
            if not self.fingers[i]:
                continue
            if _belongs(sha1_hash(self.fingers[i]), self.node_id, node_id):
                return self.fingers[i]
        return self.ip_addr

    def join(self, node_ip: str) -> Response[str]:
        """
        Join a node to a Chord ring.
        """
        self.log(f"Node [{node_ip}] is joining the ring thorugh {self.ip_addr}")
        resp = self.find_successor(self.node_id)
        if not resp[1]:
            return resp

        node_succ = resp[0]

        # I'm the successor of the node, so it is my predecessor
        if node_succ == self.ip_addr:
            logger.debug("%s is the successor of %s", self.ip_addr, node_ip)
            self.predecessor = node_ip
            pred_id = sha1_hash(self.predecessor)
            keys = [k for k in self.elems if not _belongs(k, pred_id, self.node_id)]
            if keys:
                self.log(f"Moving {len(keys)} elements out from node")
                self._update_elements(keys, self.elems)
            return new_response(self.ip_addr)

        # I'm not the successor of the node, so it will join to other node
        with DhtSession(node_succ, self.dht_id) as session:
            ret = session.join(node_succ)
        return ret

    def stabilize(self):
        """
        Verifies a node immediate successor, and tells the successor about itself.
        """
        while True:
            if self.successor != self.ip_addr:
                try:
                    with DhtSession(self.successor, self.dht_id) as session:
                        pred = session.get_predecessor()
                        if pred is not None and _belongs(
                            sha1_hash(pred), self.node_id, sha1_hash(self.successor)
                        ):
                            self.log(f"Updating successor to {pred}")
                            self.successor = pred
                        session.notify(self.ip_addr)
                except ServiceConnectionError:
                    self.successor = self.ip_addr
            time.sleep(config.DHT_STABILIZE_INTERVAL)

    def notify(self, node_ip: str) -> None:
        """
        Informs a node about a possible predecessor.

        Parameters
        ----------
        node_ip : str
        The ip of the possible predecessor node.
        """
        if self.predecessor is None or (
            node_ip != self.predecessor
            and _belongs(sha1_hash(node_ip), sha1_hash(self.predecessor), self.node_id)
        ):
            self.log(f"Node {node_ip} is the new predecessor")
            self.predecessor = node_ip

            # Move items that can be potentially stored in the new predecessor
            pred_id = sha1_hash(self.predecessor)
            keys = [k for k in self.elems if not _belongs(k, pred_id, self.node_id)]
            if keys:
                self.log(f"Moving {len(keys)} elements out from node")
                self._update_elements(keys, self.elems)

    def fix_fingers(self):
        """
        Refreshes finger table entries and stores the index of the next finger to fix.
        """
        while True:
            self.next += 1
            if self.next >= 160:
                self.next = 0
            succ, resp, _ = self.find_successor(self.node_id + (1 << self.next) - 1)
            self.fingers[self.next] = "" if not resp else succ
            time.sleep(config.DHT_FIX_FINGERS_INTERVAL)

    def _update_elements(self, elem_keys: List[int], elem_dict: Dict[int, Any]):
        """Updates the elements position in the ring."""
        for elem_key in elem_keys:
            resp = False
            while not resp:
                elem = elem_dict.get(elem_key, None)
                if elem is None:
                    continue
                _, resp, _ = self.store(elem_key, elem)
            elem_dict.pop(elem_key, None)

    def _update_repl_elements(self):
        """Updates the replica elements position in the ring."""
        self.log("Updating replica elements")
        keys = list(self.repl_elems.keys())
        if keys:
            self._update_elements(keys, self.repl_elems)

    def check_predecessor(self):
        """
        Checks whether a predecessor of a node has failed.
        """
        while True:
            if self.predecessor is not None:
                try:
                    rpyc.connect(self.predecessor, config.DHT_PORT).close()
                except:  # pylint: disable=bare-except
                    self.predecessor = None
                    self._update_repl_elements()
            time.sleep(config.DHT_CHECK_PREDECESSOR_INTERVAL)

    def find(self, elem_key: str) -> Response[Any]:
        """
        Gets an element in the node, previously found.

        Parameters
        ----------
        elem_key : int
            The key of the element in the ring.
        """
        hashed = sha1_hash(elem_key)

        # Check in replica items first
        if hashed in self.repl_elems:
            self.log(f"Found element {elem_key} in replica")
            return new_response(self.repl_elems[hashed])

        succ, resp, _ = self.find_successor(hashed)
        if not resp:
            return new_error_response("Error finding successor")
        if succ == self.ip_addr:
            self.log(f"Element {elem_key} if from this node")
            if hashed in self.elems:
                return new_response(self.elems[hashed])
            return new_response(self.repl_elems.get(hashed, None))
        self.log(f"Element {elem_key} is not from this node")
        try:
            with DhtSession(succ, self.dht_id) as session:
                return session.find(elem_key)
        except ServiceConnectionError:
            return new_error_response(f"Connection error to node {succ}")

    def store(
        self, elem_key: Union[str, int], elem: Any, overwrite: bool = True
    ) -> VoidResponse:
        """
        Stores an element in the node, previously found.

        Parameters
        ----------
        key : int
            The key of an specific elem in the ring.
        """
        # Not allowed to store None in the DHT
        if elem is None:
            return new_error_response("Element is None")

        hashed = sha1_hash(elem_key) if isinstance(elem_key, str) else elem_key
        elem_name = _get_name(elem_key)

        # Find the successor of the element
        succ, resp, _ = self.find_successor(hashed)
        if not resp:
            self.log(f"Error finding successor for {elem_name}")
            return new_error_response("Error finding successor")

        # If the successor is this node, store the element
        if succ == self.ip_addr:
            self.log(f"Storing {elem_name} in {elem_key}")
            if not overwrite and hashed in self.elems:
                self.log(f"Element {elem_name} already exists")
                return new_error_response("Element already exists")
            self.elems[hashed] = elem

            # Store replica of the element in the successor
            try:
                with DhtSession(self.successor, self.dht_id) as session:
                    session.store_replica(elem_key, elem)
            except ServiceConnectionError:
                self.log(f"Error storing replica of {elem_name}")
            return new_void_respone(msg="Element stored")

        # If the successor is not this node, order the successor to store the element
        self.log(f"Element {elem_name} is not from this node")
        try:
            with DhtSession(succ, self.dht_id) as session:
                store_resp = session.store(elem_key, elem, overwrite)
                if store_resp[1]:
                    self.log(f"{elem_name} stored in {store_resp[0]}")
                else:
                    self.log(f"Error storing {elem_name}")
                return store_resp
        except ServiceConnectionError:
            self.log(f"Connection error to node {succ}")
            return new_error_response(f"Connection error to node {succ}")

    def store_replica(self, elem_key: Union[str, int], elem: Any):
        """
        Stores a replica of an element in this node.

        Parameters
        ----------
        elem_key : int
            Key of the element to store.
        """
        self.log(f"Storing replica of {_get_name(elem_key)}")
        hashed = sha1_hash(elem_key) if isinstance(elem_key, str) else elem_key
        self.repl_elems[hashed] = elem
