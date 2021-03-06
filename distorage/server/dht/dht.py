"""
Contains a Distribute Hash Table implementation using CHORD protocol.
"""

from __future__ import annotations

import os
import threading
import time
from hashlib import sha1
from pathlib import Path
from typing import Any, Dict, List, Set, Union

import rpyc

from distorage.exceptions import ServiceConnectionError
from distorage.logger import logger
from distorage.response import (
    Response,
    VoidResponse,
    new_error_response,
    new_response,
    new_void_response,
)
from distorage.server import config
from distorage.server.dht.dht_id_enum import DhtID
from distorage.server.dht.dht_session import DhtSession

# pylint: disable=too-many-arguments
# pylint: disable=too-many-return-statements


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


def _is_path(value) -> bool:
    try:
        path = Path(value)
        return path.exists() and path.is_file()
    except Exception:  # pylint: disable=broad-except
        return False


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
        self._predecessor: Union[str, None] = None
        self._successor: str = ip_addr
        self.fingers: List[str] = [""] * 160
        self.next: int = -1
        self.elems: Dict[int, Any] = {}
        self.repl_elems: Dict[int, Any] = {}
        self.removed_elems: Set[int] = set()
        self.run_coroutines()

    @property
    def predecessor(self) -> Union[str, None]:
        """Returns the predecessor node of the node."""
        return self._predecessor

    @predecessor.setter
    def predecessor(self, predecessor: Union[str, None]):
        """
        Sets the predecessor node of the node.

        Parameters
        ----------
        predecessor : str
            The predecessor node of the node.
        """
        if predecessor == self._predecessor:
            return

        if predecessor is None:
            for key, val in self.repl_elems.items():
                self.elems[key] = val
            self.repl_elems = {}

        self.log(f"Updating predecessor to {predecessor}")
        self._predecessor = predecessor

        if predecessor is None:
            return

        # Move items that can be potentially stored in the new predecessor
        self._fix_elem_dict()
        self._update_repl_elements()

    @property
    def successor(self) -> str:
        """Returns the successor node of the node."""
        return self._successor

    @successor.setter
    def successor(self, successor: str):
        """
        Sets the successor node of the node.

        Parameters
        ----------
        successor : str
            The successor node of the node.
        """
        if successor == self._successor:
            return

        if successor == self.ip_addr:
            for key, val in self.repl_elems.items():
                self.elems[key] = val
            self.repl_elems = {}

        self.log(f"Updating successor to {successor}")
        self._successor = successor

        if successor == self.ip_addr:
            return

        # Move items that can be potentially stored in the new predecessor
        self._fix_elem_dict()
        self._resend_replica_to_successor()

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
            self.predecessor = node_ip
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
            self.predecessor = node_ip

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

    def _fix_elem_dict(self):
        """Checks if there are elements that don't belong here."""
        if self.predecessor is None:
            return
        pred_id = sha1_hash(self.predecessor)
        keys = [k for k in self.elems if not _belongs(k, pred_id, self.node_id)]
        if keys:
            self.log(f"Moving {len(keys)} elements out from node")
            self._update_elements(keys, self.elems)

    def _update_elements(self, elem_keys: List[int], elem_dict: Dict[int, Any]):
        """Updates the elements position in the ring."""
        for elem_key in elem_keys:
            resp = False
            while not resp:
                elem = elem_dict.get(elem_key, None)
                if elem is None:
                    continue
                if _is_path(elem):
                    with open(elem, "rb") as file:
                        file_bytes = file.read()
                    _, resp, _ = self.store(
                        elem_key, file_bytes, check_removed=True, persist_path=elem
                    )
                    if resp:
                        os.remove(elem)
                else:
                    _, resp, _ = self.store(elem_key, elem, check_removed=True)
            elem_dict.pop(elem_key, None)

    def _update_repl_elements(self):
        """Updates moves all the replica elements to the new successor."""
        self.log("Updating replica elements")
        keys = list(self.repl_elems.keys())
        if keys:
            self._update_elements(keys, self.repl_elems)

    def _resend_replica_to_successor(self):
        """Resends all the elements as replica to the successor."""
        self.log("Resending elements to successor")
        keys = list(self.elems.keys())
        for key in keys:
            try:
                with DhtSession(self.successor, self.dht_id) as session:
                    val = self.elems.get(key, None)
                    if val is None:
                        continue
                    path = _is_path(val)
                    if path is not None:
                        with open(path, "rb") as file:
                            file_bytes = file.read()
                        session.store_replica(key, file_bytes, persist_path=val)
                    else:
                        session.store_replica(key, val)
            except ServiceConnectionError:
                self.log(f"Failed to send element {key} to successor")
                continue

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

    def _save_element(self, elem: bytes, persist_path: str) -> str:
        path = Path(persist_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as file:
            file.write(elem)
        return persist_path

    def _load_element(self, elem_path: str) -> Union[bytes, None]:
        path = Path(elem_path)
        if not path.exists() or not path.is_file():
            return None
        with open(elem_path, "rb") as file:
            return file.read()

    def find(self, elem_key: str, is_file: bool = False) -> Response[Any]:
        """
        Gets an element in the node, previously found.

        Parameters
        ----------
        elem_key : int
            The key of the element in the ring.
        from_path : str
            If given, the element will be loaded from the given path.
        """
        hashed = sha1_hash(elem_key)

        # Check in replica items first
        if hashed in self.repl_elems:
            self.log(f"Found element {elem_key} in replica")
            elem = self.repl_elems[hashed]
            if is_file:
                elem = self._load_element(elem)
            return new_response(elem)

        succ, resp, _ = self.find_successor(hashed)
        if not resp:
            return new_error_response("Error finding successor")

        if succ == self.ip_addr:
            elem = None
            if elem_key not in self.removed_elems:
                if hashed in self.elems:
                    self.log(f"Found element {elem_key} in local storage")
                    elem = self.elems[hashed]
                elif hashed in self.repl_elems:
                    self.log(f"Found element {elem_key} in replica")
                    elem = self.repl_elems[hashed]

                if is_file and elem is not None:
                    self.log(f"Loading element {elem} from disk")
                    elem = self._load_element(elem)
            return new_response(elem)

        self.log(f"Element {elem_key} is not from this node")
        try:
            with DhtSession(succ, self.dht_id) as session:
                return session.find(elem_key, is_file)
        except ServiceConnectionError:
            return new_error_response(f"Connection error to node {succ}")

    def store(
        self,
        elem_key: Union[str, int],
        elem: Any,
        overwrite: bool = True,
        check_removed: bool = False,
        persist_path: Union[str, None] = None,
    ) -> VoidResponse:
        """
        Stores an element in the node, previously found.

        Parameters
        ----------
        elem_key : int
            The key of an specific elem in the ring.
        elem : Any
            The element to be stored.
        overwrite : bool
            Whether to overwrite an existing element.
        check_removed : bool
            Whether to check if the element was removed in the past.

            If True, and the element was removed in the past, it will
            not be stored.
        persist_path : str
            If not None, the element will be stored in the given path.
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

        # If the successor is this node, store the element here
        if succ == self.ip_addr:
            self.log(f"Storing {elem_name} in {elem_key}")

            # Check if the element already exists and overwrite is not allowed
            if not overwrite and hashed in self.elems:
                self.log(f"Element {elem_name} already exists")
                return new_error_response("Element already exists")

            # Check if the element was removed in the past
            was_removed = hashed in self.removed_elems
            if check_removed and was_removed:
                self.log(f"Element {elem_name} was removed in the past")
                return new_void_response(msg="Element was removed in the past")

            # Check if the element must be saved in disk
            saved_elem = elem
            if persist_path is not None:
                self.log(f"Writing {elem_name} in {persist_path}")
                saved_elem = self._save_element(elem, persist_path)
            self.elems[hashed] = saved_elem

            # Element is back in the system so update the removed set
            if was_removed:
                self.removed_elems.remove(hashed)

            # Store replica of the element in the successor
            try:
                if self.successor != self.ip_addr:
                    with DhtSession(self.successor, self.dht_id) as session:
                        session.store_replica(elem_key, elem, persist_path)
            except ServiceConnectionError:
                self.log(f"Error storing replica of {elem_name}")
            return new_void_response(msg="Element stored")

        # If the successor is not this node, order the successor to store the element
        self.log(f"Element {elem_name} is not from this node")
        try:
            with DhtSession(succ, self.dht_id) as session:
                store_resp = session.store(
                    elem_key, elem, overwrite, check_removed, persist_path
                )
                if not store_resp[1]:
                    self.log(f"Error storing {elem_name}")
                return store_resp
        except ServiceConnectionError:
            self.log(f"Connection error to node {succ}")
            return new_error_response(f"Connection error to node {succ}")

    def store_replica(
        self,
        elem_key: Union[str, int],
        elem: Any,
        persist_path: Union[str, None] = None,
    ) -> VoidResponse:
        """
        Stores a replica of an element in this node.

        Parameters
        ----------
        elem_key : int
            Key of the element to store.
        """
        self.log(f"Storing replica of {_get_name(elem_key)}")
        hashed = sha1_hash(elem_key) if isinstance(elem_key, str) else elem_key
        if persist_path is not None:
            elem = self._save_element(elem, persist_path)
        self.repl_elems[hashed] = elem
        return new_void_response(msg="Replica stored")

    def remove(self, elem_key: Union[str, int]):
        """
        Removes an element from the node.

        Parameters
        ----------
        elem_key : int
            Key of the element to remove.
        """
        hashed = sha1_hash(elem_key) if isinstance(elem_key, str) else elem_key
        elem_name = _get_name(elem_key)

        # Find the successor of the element
        succ, resp, _ = self.find_successor(hashed)
        if not resp:
            self.log(f"Error finding successor for {elem_name}")
            return new_error_response("Error finding successor")

        # If the successor is this node, remove the element
        if succ == self.ip_addr:
            self.log(f"Removing {elem_name}")
            self.elems.pop(hashed, None)
            self.repl_elems.pop(hashed, None)
            self.removed_elems.add(hashed)
            try:
                with DhtSession(self.successor, self.dht_id) as session:
                    session.remove_replica(elem_key)
            except ServiceConnectionError:
                self.log(f"Error removing replica of {elem_name}")
            return new_void_response(msg="Element removed")

        # If the successor is not this node, order the successor to remove the element
        self.log(f"Element {elem_name} is not from this node")
        try:
            with DhtSession(succ, self.dht_id) as session:
                return session.remove(elem_key)
        except ServiceConnectionError:
            self.log(f"Connection error to node {succ}")
            return new_error_response(f"Connection error to node {succ}")

    def remove_replica(self, elem_key: Union[str, int]) -> VoidResponse:
        """
        Removes a replica of an element from this node.

        Parameters
        ----------
        elem_key : int
            Key of the element to remove.
        """
        hashed = sha1_hash(elem_key) if isinstance(elem_key, str) else elem_key
        self.repl_elems.pop(hashed, None)
        return new_void_response(msg="Replica removed")
