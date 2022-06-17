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

from typing import Any, Optional

import rpyc

from distorage.server import config
from distorage.server.client_session import ClientSessionService


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
        self._conn: Optional[rpyc.Connection] = None

    @property
    def _root(self) -> Any:
        assert (
            self._conn is not None and self._conn.root is not None
        ), "Not connected to server"
        return self._conn.root

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
        return self._root.register(self._name, self._pass)

    def login(self):
        """Logs in to the server."""
        return self._root.login(self._name, self._pass)

    def upload(self, file_path: str):
        """
        Uploads a file to the server.

        Parameters
        ----------
        file_path : str
            The path of the file to upload.
        """
        with open(file_path, "rb") as file:
            file_data = file.read()
        self._root.upload(file_data)

    def download(self, file_path: str, dest_path: str):
        """
        Downloads a file from the server.

        Parameters
        ----------
        folder_path : str
            The path of the file in the server.
        dest_path : str
        """
        file_data = self._root.download(file_path)
        with open(dest_path, "wb") as file:
            file.write(file_data)
        return file_data

    def delete(self, file_name: str):
        """
        Deletes a file from the server.

        Parameters
        ----------
        file_name : str
            The name of the file to delete.
        """
        return self._root.delete(file_name)

    def list_files(self):
        """
        Lists the files in the server.
        """
        return self._root.list_files()
