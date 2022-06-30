"""
This contains the client sessions service class.

This rpyc service is responsible for managing client sessions on the server.
"""

import json
from typing import Any, List, Union

import rpyc

from distorage.response import (
    Response,
    VoidResponse,
    new_error_response,
    new_void_respone,
)
from distorage.server.server_manager import ServerManager


def _ensure_registered(func):
    def wrapper(self, *args, **kwargs):
        # pylint: disable=protected-access
        if self._username is None or self._passwd is None:
            return False, "You are not registered"
        return func(self, *args, **kwargs)

    return wrapper


class ClientSessionService(rpyc.Service):
    """This class is responsible for managing client sessions on the server."""

    def __init__(self):
        self._username: Union[str, None] = None
        self._passwd: Union[str, None] = None

    @property
    def username(self) -> str:
        """Returns the username of the client."""
        assert self._username is not None, "Not logged in"
        return self._username

    @property
    def passwd(self) -> str:
        """Returns the password of the client."""
        assert self._passwd is not None, "Not logged in"
        return self._passwd

    def exposed_register(self, username: str, password: str) -> VoidResponse:
        """
        Registers a new user.

        Parameters
        ----------
        username : str
            The name of the new user.
        password : str
            The password of the new user.
        """
        clients = ServerManager.clients_dht()
        client_info = {
            "passwd": hash(password),
            "files": [],
        }
        resp = clients.store(username, json.dumps(client_info), overwrite=False)
        if resp[1]:
            self._username = username
            self._passwd = password
        return resp

    def exposed_login(self, username: str, password: str) -> VoidResponse:
        """
        Logins a user.

        Parameters
        ----------
        username : str
            The name of the user.
        password : str
            The password of the user.
        """
        clients = ServerManager.clients_dht()
        val, resp, _ = clients.find(username)
        if not resp or val is None:
            return new_error_response("Failed to login. Try again later.")
        client_info = json.loads(val)
        if client_info["passwd"] != hash(password):
            return new_error_response("Wrong password")
        self._username = username
        self._passwd = password
        return new_void_respone()

    @_ensure_registered
    def exposed_upload(self, file: List[bytes], sys_path: str) -> VoidResponse:
        """
        Uploads a file.

        Parameters
        ----------
        file : List[bytes]
            The file to upload.
        sys_path : str
            The path of the file on the system.

        Returns
        -------
        bool
            Whether the upload was successful.
        str
            The error message if the upload was not successful.
        """
        elem_key = f"{self.username}:{sys_path}"
        elem_val = file
        data_dht = ServerManager.data_dht()
        client_dht = ServerManager.clients_dht()
        val, resp, msg = client_dht.find(self.username)
        if not resp:
            return new_error_response(msg)
        client_info = json.loads(val)
        client_info["files"].append(elem_key)
        cli_resp = client_dht.store(
            self.username, json.dumps(client_info), overwrite=True
        )
        if not cli_resp[1]:
            return cli_resp
        return data_dht.store(elem_key, elem_val, overwrite=False)

    @_ensure_registered
    def exposed_download(self, file_name: str) -> Response[Any]:
        """
        Downloads a file.

        Parameters
        ----------
        file_name : str
            The name of the file to download.

        Returns
        -------
        bool
            Whether the download was successful.
        List[bytes]
            The file if the download was successful.
        """
        data_dht = ServerManager.data_dht()
        elem_key = f"{self.username}:{file_name}"
        return data_dht.find(elem_key)

    @_ensure_registered
    def exposed_delete(self, file_name: str):
        """
        Deletes a file.

        Parameters
        ----------
        file_name : str
            The name of the file to delete.
        """
        raise NotImplementedError()

    @_ensure_registered
    def exposed_list_files(self):
        """
        Lists all files.

        Returns
        -------
        List[str]
            The names of all files.
        """
        raise NotImplementedError()
