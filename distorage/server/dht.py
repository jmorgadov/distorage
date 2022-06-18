"""
Contains a Distribute has table implemantation using CHORD protocol
"""
from __future__ import annotations

from hashlib import sha1
from typing import Any, List


class ChordNode:
    """
    A Node based on CHORD protocol.
    """

    def __init__(self, ip_addr: str):
        self.ip_addr: str = ip_addr
        self.node_id: int = hash_info(ip_addr)
        self.predecessor: str = ip_addr
        self.successor: str = None
        self.fingers: List[str] = [None] * 64
        self.next: int = -1
        self.elems = {}

    def find_successor(self, node_id: int) -> str:
        """
        Find the successor node of a specific id.

        Parameters
        ----------
        node_id : int
            The key to find it's successor in a CHORD ring.
        """
        if _belongs(node_id, self.node_id, hash_info(self.successor)):
            return self.successor
        raise NotImplementedError()
        # closet = self.closest_preceding_node(node_id)
        # Conect to closet(str type) and call its find_find_successor with node_id
        # return closet.find_successor(node_id)

    def closest_preceding_node(self, node_id: int) -> str:
        """
        Find the closest preceding node of a specific id.

        Parameters
        ----------
        node_id : int
            The key to find it's successor in a CHORD ring.
        """
        for i in range(len(self.fingers), 0, -1):
            if not self.fingers[i]:
                continue
            if _belongs(hash_info(self.fingers[i]), self.node_id, node_id):
                return self.fingers[i]
        return self

    # Add a node_ip: str argument to the function
    def join(self) -> None:
        """
        Join a node to a Chord ring.
        """
        self.predecessor = None
        # Conect to node_ip an call find_successor with self.node_ip previously hashed
        # self.successor = external_node_find_successor(hash_info(self.node_ip))

    # Call periodically
    def stabilize(self) -> None:
        """
        Verifies a node immediate successor, and tells the successor about itself.
        """
        # Conect to successor and ask for it's predecessor ip
        # temp = self.successor.predecessor
        # if _belongs(hash_info(temp), self.node_id, hash_info(self.successor)):
        #    self.successor = temp
        # Conect to succesor an call notify with self.node_ip
        # self.successor.notify(self.node_ip)
        raise NotImplementedError()

    def notify(self, node_ip: str) -> None:
        """
        Informs a node about a possible predecessor.

        Parameters
        ----------
        node_ip : str
        The ip of the possible predecessor node.
        """
        if not self.predecessor or _belongs(
            hash_info(node_ip), hash_info(self.predecessor), self.node_id
        ):
            self.predecessor = node_ip

    # Call periodically
    def fix_fingers(self):
        """
        Refreshes finger table entries and stores the index of the next finger to fix.
        """
        self.next += 1
        if self.next > 160:
            self.next = 0
        self.fingers[self.next] = self.find_successor(self.node_id + 1 << self.next - 1)

    # Call periodically
    def check_predecessor(self):
        """
        Checks whether a predecessor of a node has failed.

        """
        # if predecessor doesn't respond(PING till respond just 2 times)
        #   predecesor  = None
        raise NotImplementedError()

    def find_location(self, key: int) -> Any:
        """
        Finds where a key should be located in the ring.

        Parameters
        ----------
        key : int
            The key value to find in the ring.
        """
        return self.find_successor(key)

    def store(self, elem: Any, elem_key: int):
        """
        Stores an element in the node, previously found.

        Parameters
        ----------
        key : int
            The key of an specific elem in the ring.
        """
        self.elems[elem_key] = elem

    def get_elem(self, elem_key: int) -> Any:
        """
        Gets an element in the node, previously found.

        Parameters
        ----------
        elem_key : int
            The key of the element in the ring.
        """
        return self.elems[elem_key]


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


def hash_info(value: str) -> int:
    """
    Aply a hash function (SHA-1) to a string value

    Parameters
    ----------
    value : str
        The value to be hashed.
    """
    _hs = sha1(value.encode("utf-8")).digest()
    return int.from_bytes(_hs, ordering="big")
