from operator import le
from typing import List

class ChordNode:
    
    def __init__(self, ip, id):
        self.ip = ip
        self.id = id
        self.fingers = [None]*64
        self.next_finger = 0
        self.predecessor, self.successor = None, None
    
    
    def find_successor(self, id):
        if self.id < id <= self.successor.id:
            return self.successor
        closet = self.closest_preceding_node(id)
        return closet.find_successor(id)
        

    def closest_preceding_node(self, id):
        for i in range(len(self.fingers), 0, -1):
            if not self.fingers[i]:
                continue
            if self.id < self.fingers[i] < id:
                return self.fingers[i]
        return self
    
    
    def join(self, node):
        self.predecessor = None
        self.successor = node.find_successor(self.id)
    
    
    def stabilize(self):
        temp = self.successor.predecessor
        if self.id < temp.id < self.successor.id:
            self.successor = temp
        self.successor.notify(self)
    
    
    def notify(self, node):
        if  not self.predecessor or self.predecessor.id < node.id < self.id:
            self.predecessor = node
    
    
    def fix_fingers():
        raise NotImplementedError()
    
    
    def check_predecessor():
        raise NotImplementedError()
