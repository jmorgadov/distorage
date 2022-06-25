"""
Contains a Distribute Hash Table implementation using CHORD protocol.
"""

from __future__ import annotations

import threading
import time
from hashlib import sha1
from typing import Any, Dict, List, Union

import rpyc

from distorage.logger import logger
from distorage.server import config
from distorage.server.dht import DhtID, DhtSession


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

    def find_successor(self, node_id: int) -> str:
        """
        Find the successor node of a specific id.

        Parameters
        ----------
        node_id : int
            The key to find it's successor in a CHORD ring.
        """
        if _belongs(node_id, self.node_id, sha1_hash(self.successor)):
            return self.successor

        closest = self.closest_preceding_node(node_id)
        if closest == self.ip_addr:
            return self.ip_addr
        with DhtSession(closest, self.dht_id) as session:
            succ = session.find_successor(node_id)
        return succ

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

    def join(self, node_ip: str) -> str:
        """
        Join a node to a Chord ring.
        """
        self.log(f"Node [{node_ip}] is joining the ring thorugh {self.ip_addr}")
        node_succ = self.find_successor(self.node_id)

        # I'm the successor of the node, so it could be a predecessor
        if node_succ == self.ip_addr:
            logger.debug("%s is the successor of %s", self.ip_addr, node_ip)
            self.predecessor = node_ip
            return self.ip_addr

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
                with DhtSession(self.successor, self.dht_id) as session:
                    pred = session.get_predecessor()
                    if pred is not None and _belongs(
                        sha1_hash(pred), self.node_id, sha1_hash(self.successor)
                    ):
                        self.successor = pred
                    session.notify(self.ip_addr)
            time.sleep(config.DHT_STABILIZE_INTERVAL)

    def notify(self, node_ip: str) -> None:
        """
        Informs a node about a possible predecessor.

        Parameters
        ----------
        node_ip : str
        The ip of the possible predecessor node.
        """
        if not self.predecessor or _belongs(
            sha1_hash(node_ip), sha1_hash(self.predecessor), self.node_id
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
            self.fingers[self.next] = self.find_successor(
                self.node_id + (1 << self.next) - 1
            )
            time.sleep(config.DHT_FIX_FINGERS_INTERVAL)

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
            time.sleep(config.DHT_CHECK_PREDECESSOR_INTERVAL)

    def find(self, elem_key: str) -> Any:
        """
        Gets an element in the node, previously found.

        Parameters
        ----------
        elem_key : int
            The key of the element in the ring.
        """
        hashed = sha1_hash(elem_key)
        succ = self.find_successor(hashed)
        if succ == self.ip_addr:
            self.log(f"Element {elem_key} if from this node")
            return self.elems.get(hashed, None)
        self.log(f"Element {elem_key} is not from this node")
        with DhtSession(succ, self.dht_id) as session:
            val = session.find(elem_key)
        return val

    def store(self, elem_key: str, elem: Any, overwrite: bool = True) -> bool:
        """
        Stores an element in the node, previously found.

        Parameters
        ----------
        key : int
            The key of an specific elem in the ring.
        """
        hashed = sha1_hash(elem_key)
        succ = self.find_successor(hashed)
        if succ == self.ip_addr:
            self.log(f"Storing {str(elem)} in {elem_key}")
            if not overwrite and hashed in self.elems:
                self.log(f"Element {elem_key} already exists")
                return False
            self.elems[hashed] = elem
            return True
        self.log(f"Element {elem_key} is not from this node")
        with DhtSession(succ, self.dht_id) as session:
            val = session.store(elem_key, elem, overwrite)
        return val
