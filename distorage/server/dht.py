from __future__ import annotations
from hashlib import sha1
from sys import hash_info

from typing import Generic, List, Optional, TypeVar

T = TypeVar("T")

class DHT(Generic[T]):
    def find(self, table_key: int) -> Optional[T]:
        raise NotImplementedError()


class ChordNode:
    def __init__(self, ip_addr: str):
        self.ip_addr: str = ip_addr
        self.node_id: int = hash_info(ip_addr)
        self.predecessor: str = ip_addr
        self.successor: str = None
        self.fingers: List[int] = [0] * 64
        self.next: int = 0
    
    def find_successor(self, node_id: int)-> str:
        if _belongs(node_id, self.node_id, hash_info(self.successor), True):
            return self.successor
        closet = self.closest_preceding_node(node_id)
        return closet.find_successor(node_id)

    def closest_preceding_node(self, node_id: int) -> ChordNode:
        for i in range(len(self.fingers), 0, -1):
            if not self.fingers[i]:
                continue
            if _belongs(self.fingers[i], self.node_id, node_id, False):
                return self.fingers[i]
        return self

    def join(self, node: ChordNode) -> None:
        self.predecessor = None
        self.successor = node.find_successor(self.node_id)

    def stabilize(self) -> None:
        # Conect to successor and ask for it's predecessor ip
        #temp = self.successor.predecessor
        #if _belongs(temp.node_id, self.node_id, self.successor.node_id, False):
        #    self.successor = temp
        #self.successor.notify(self)
        raise NotImplementedError()

    def notify(self, node_ip: str) -> None:
        if (
            not self.predecessor
            or _belongs(hash_info(node_ip), hash_info(self.predecessor), self.node_id)
        ):
            self.predecessor = node_ip

    # Call periodically
    def fix_fingers(self):
        self.next += 1
        if self.next > 160:
            self.next = 0
        self.fingers[self.next] = self.find_successor(self.node_id + 1<<self.next-1)

    # Call periodically, checks whether predecessor has failed.
    def check_predecessor(self): #TODO: implement
        # if predecessor doesn't respond(PING till respond)
        #   predecesor  = None
        raise NotImplementedError()
        


def _belongs(value: int, lower: int, upper: int, include_upper: bool)-> bool:
        result = False
        result = result or (lower < value <= upper )
        result = result or (lower < value < upper)
        result = result or (lower > value <= upper and lower > upper)
        result = result or (lower > value < upper and lower > upper)
        return (result and include_upper) if include_upper else result


def hash_info(value: str)-> int:
    value = value.encode('utf-8')
    hs = sha1(value).digest()
    return int.frombytes(hs,ordering="big")