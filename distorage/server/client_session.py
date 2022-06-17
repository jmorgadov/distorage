from typing import List, Union, Tuple

import rpyc


def _ensure_registered(func):
    def wrapper(self, *args, **kwargs):
        if self.username is None or self.password is None:
            return False, "You are not registered"
        return func(self, *args, **kwargs)
    return wrapper


class ClientSessionService(rpyc.Service):
    def __init__(self):
        self.username: Union[str, None] = None
        self.passwd: Union[str, None] = None

    def expose_register(self, username: str, password: str):
        raise NotImplementedError()

    def expose_login(self, username: str, password: str):
        raise NotImplementedError()

    @_ensure_registered
    def expose_upload(self, file: List[bytes]) -> Tuple[bool, str]:
        raise NotImplementedError()

    @_ensure_registered
    def expose_download(self, file_name: str) -> Tuple[bool, List[bytes]]:
        raise NotImplementedError()

    @_ensure_registered
    def expose_delete(self, file_name: str):
        raise NotImplementedError()

    @_ensure_registered
    def expose_list_files(self):
        raise NotImplementedError()
