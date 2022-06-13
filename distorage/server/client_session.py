import logging
import sys
from typing import List, Optional, Tuple

import rpyc

from distorage.server import config


def ensure_registered(func):
    def wrapper(self, *args, **kwargs):
        if self.username is None or self.password is None:
            return False, "You are not registered"
        return func(self, *args, **kwargs)
    return wrapper


class ClientSessionService(rpyc.Service):
    def __init__(self):
        self.username: Optional[str] = None
        self.passwd: Optional[str] = None

    def expose_register(self, username: str, password: str):
        self.username = username
        self.passwd = password
        # TODO: look in a DHT if the user exists, else, create it
        raise NotImplementedError()

    @ensure_registered
    def expose_upload(self, file: List[bytes]) -> Tuple[bool, str]:
        raise NotImplementedError()

    @ensure_registered
    def expose_download(self, file_name: str) -> Tuple[bool, List[bytes]]:
        raise NotImplementedError()

    @ensure_registered
    def expose_delete(self, file_name: str):
        raise NotImplementedError()

    @ensure_registered
    def expose_list_files(self):
        raise NotImplementedError()

class ClientSession:
    def __init__(self, username: str, password: str, ip_addr: str):
        self.username = username
        self.password = password
        self.ip_addr = ip_addr
        self.conn = None

    def __enter__(self) -> ClientSessionService:
        self.conn = rpyc.connect(self.ip_addr, config.CLIENT_PORT)
        assert self.conn is not None
        ret, msg = self.conn.root.register(self.username, self.password)
        if not ret:
            logging.error(msg)
            sys.exit(1)
        return self.conn.root

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self.conn is not None
        self.conn.close()
