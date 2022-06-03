from typing import BinaryIO


class RemoteSession:

    def __init__(self, username: str, password: str):
        self._name = username
        self._pass = password

    def upload_file(self, file: BinaryIO):
        raise NotImplementedError()

    def download_file(self, file_name: str):
        raise NotImplementedError()

    def delete_file(self, file_name: str):
        raise NotImplementedError()

    def list_files(self):
        raise NotImplementedError()
