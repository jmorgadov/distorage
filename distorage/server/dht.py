from __future__ import annotations

from typing import Generic, List, Optional, TypeVar

T = TypeVar("T")

class DHT(Generic[T]):
    def find(self, table_key: int) -> Optional[T]:
        raise NotImplementedError()


class ChordNode:
    def __init__(self, ip_addr, node_id: int):
        self.ip_addr = ip_addr
        self.node_id: int = node_id
        self.fingers: List[Optional[int]] = [None] * 64
        self.next_finger: int = 0
        self.predecessor: Optional[ChordNode] = None
        self.successor: Optional[ChordNode] = None

    def find_successor(self, node_id: int):
        # TODO: check what happens if self.successor is None
        if self.node_id < node_id <= self.successor.node_id:
            return self.successor
        closet = self.closest_preceding_node(node_id)
        return closet.find_successor(node_id)

    def closest_preceding_node(self, node_id: int) -> ChordNode:
        for i in range(len(self.fingers), 0, -1):
            if not self.fingers[i]:
                continue
            # TODO: fingers[i] might be None
            if self.node_id < self.fingers[i] < node_id:
                return self.fingers[i]
        return self

    def join(self, node: ChordNode) -> None:
        self.predecessor = None
        self.successor = node.find_successor(self.node_id)

    def stabilize(self) -> None:
        # TODO: check what happens if self.successor is None
        temp = self.successor.predecessor
        if self.node_id < temp.node_id < self.successor.node_id:
            self.successor = temp
        self.successor.notify(self)

    def notify(self, node: ChordNode) -> None:
        if (
            not self.predecessor
            or self.predecessor.node_id < node.node_id < self.node_id
        ):
            self.predecessor = node

    def fix_fingers(self):
        raise NotImplementedError()

    def check_predecessor(self):
        raise NotImplementedError()
