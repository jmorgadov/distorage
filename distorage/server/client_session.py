"""
This contains the client sessions service class.

This rpyc service is responsible for managing client sessions on the server.
"""

from typing import List, Tuple, Union

import rpyc


def _ensure_registered(func):
    def wrapper(self, *args, **kwargs):
        if self.username is None or self.password is None:
            return False, "You are not registered"
        return func(self, *args, **kwargs)

    return wrapper


class ClientSessionService(rpyc.Service):
    """This class is responsible for managing client sessions on the server."""

    def __init__(self):
        self.username: Union[str, None] = None
        self.passwd: Union[str, None] = None

    def expose_register(self, username: str, password: str):
        """
        Registers a new user.

        Parameters
        ----------
        username : str
            The name of the new user.
        password : str
            The password of the new user.
        """
        raise NotImplementedError()

    def expose_login(self, username: str, password: str):
        """
        Logins a user.

        Parameters
        ----------
        username : str
            The name of the user.
        password : str
            The password of the user.
        """
        raise NotImplementedError()

    @_ensure_registered
    def expose_upload(self, file: List[bytes]) -> Tuple[bool, str]:
        """
        Uploads a file.

        Parameters
        ----------
        file : List[bytes]
            The file to upload.

        Returns
        -------
        bool
            Whether the upload was successful.
        str
            The error message if the upload was not successful.
        """
        raise NotImplementedError()

    @_ensure_registered
    def expose_download(self, file_name: str) -> Tuple[bool, List[bytes]]:
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
        raise NotImplementedError()

    @_ensure_registered
    def expose_delete(self, file_name: str):
        """
        Deletes a file.

        Parameters
        ----------
        file_name : str
            The name of the file to delete.
        """
        raise NotImplementedError()

    @_ensure_registered
    def expose_list_files(self):
        """
        Lists all files.

        Returns
        -------
        List[str]
            The names of all files.
        """
        raise NotImplementedError()
