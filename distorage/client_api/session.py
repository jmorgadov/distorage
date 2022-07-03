"""
This will contain an API with the main functionalities accesible by a client:

- Log in
- Log out
- Upload file
- Download file
- View uploaded files
- Remove uploaded file

These functions will handle all the client-server comunication. This layer of
absrtaction allows the implementation of CLI or GUI applications in an easy and
clean way.
"""

from pathlib import Path
from random import randint
from typing import Any, List, Union

import rpyc

from distorage.response import (
    Response,
    VoidResponse,
    new_error_response,
    new_void_response,
)
from distorage.server import config


servers_on = []

class ClientSession:
    """
    Handles a client session with the server.

    Parameters
    ----------
    username : str
        The username of the client.
    password : str
        The password of the client.
    """

    def __init__(self, username: str, password: str):
        self._name = username
        self._pass = password
        self._conn: Union[rpyc.Connection, None] = None

    @property
    def _root(self) -> Any:
        global servers_on
        while True:
            try:
                self._conn.root.ping()
                servers_on = self._conn.root.avaiable_servers()
                return self._conn.root
            except:
                serv = servers_on[randint(0, len(servers_on)-1)]
                self.connect(serv)


    def connect(self, ip_addr: str):
        """
        Connects to the server.

        Parameters
        ----------
        ip_addr : str
            The IP address of the server.
        """
        self._conn = rpyc.connect(ip_addr, config.CLIENT_PORT)

    def disconnect(self):
        """Disconnects from the server."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def register(self):
        """Register to the system as a new user."""
        self._root.avaiable_servers()
        return self._root.register(self._name, self._pass)

    def login(self):
        """Logs in to the server."""
        return self._root.login(self._name, self._pass)

    def upload(self, file_path: str, sys_path: str) -> VoidResponse:
        """
        Uploads a file to the server.

        Parameters
        ----------
        file_path : str
            The path of the file to upload.
        sys_path : str
            The path of the file in the server.
        """
        path = Path(file_path)
        if not path.exists():
            return new_error_response("File does not exist")
        with open(file_path, "rb") as file:
            data = file.read()
        assert isinstance(data, bytes), "Data is not bytes"
        return self._root.upload(data, sys_path)

    def download(self, file_path: str, dest_path: str) -> VoidResponse:
        """
        Downloads a file from the server.

        Parameters
        ----------
        folder_path : str
            The path of the file in the server.
        dest_path : str
        """
        resp = self._root.download(file_path)
        if not resp[1]:
            return resp
        if resp[0] is None:
            return new_error_response(msg=f"File {file_path} does not exist")
        path = Path(dest_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(dest_path, "wb") as file:
            file.write(resp[0])
        return new_void_response(msg="File downloaded successfully")

    def delete(self, file_name: str):
        """
        Deletes a file from the server.

        Parameters
        ----------
        file_name : str
            The name of the file to delete.
        """
        return self._root.delete(file_name)

    def list_files(self) -> Response[List[str]]:
        """
        Lists the files in the server.
        """
        return self._root.list_files()
