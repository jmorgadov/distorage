"""
Contains the main program for the Distorage client.
"""

import os
import sys
import time
from getpass import getpass
from typing import Callable, Tuple, Union

from distorage.client_api.session import ClientSession


def _clear():
    os.system("cls" if os.name == "nt" else "clear")


def prompt(*options: Tuple[str, Callable], init_msg: str = "") -> str:
    """
    Prompts the user to select an option from a list of options.
    """
    _clear()
    if init_msg:
        print(init_msg)
    print("Please select an option:")
    for i, option in enumerate(options):
        print(f"{i + 1:>2}) {option[0]}")
    while True:
        try:
            choice = int(input("Choice: "))
            if 1 <= choice <= len(options):
                return options[choice - 1][1]()
            raise ValueError
        except ValueError:
            print("Invalid choice.")
            time.sleep(2)


class ClientPrompt:
    """
    This is a helper class for the client prompt.
    """

    def __init__(self):
        self._session: Union[ClientSession, None] = None

    @property
    def session(self) -> ClientSession:
        """Returns the current session."""
        assert self._session is not None
        return self._session

    def _exit(self):
        _clear()
        sys.exit(0)

    def _connect(self, client_name: str, client_pass: str, server_addr: str):
        _clear()
        print(f"Connecting to server {server_addr}...")
        self._session = ClientSession(client_name, client_pass)
        self._session.connect(server_addr)
        self._initial_promp()

    def _disconnect(self):
        self.session.disconnect()
        self._initial_promp()

    def _login(self):
        ret, msg = self.session.login()
        if not ret:
            print(f"Login failed: {msg}")
            time.sleep(2)
            self._initial_promp()
        else:
            self._distorage()

    def _register(self):
        ret, msg = self.session.register()
        if not ret:
            print(f"Registration failed: {msg}")
            time.sleep(2)
            self._initial_promp()
        else:
            self._distorage()

    def _upload_file(self):
        raise NotImplementedError()

    def _download_file(self):
        raise NotImplementedError()

    def _list_files(self):
        raise NotImplementedError()

    def _delete_file(self):
        raise NotImplementedError()

    def _distorage(self):
        """
        This function ask for an operation from the user and execute it.
        """
        _clear()
        prompt(
            ("Upload file", self._upload_file),
            ("Download file", self._download_file),
            ("List files", self._list_files),
            ("Delete file", self._delete_file),
            ("Disconnect", self._disconnect),
            init_msg="Welcome to Distorage!",
        )

    def _initial_promp(self):
        _clear()
        prompt(
            ("Login", self._login),
            ("Register", self._register),
            ("Back", self.run),
            ("Exit", self._exit),
        )

    def run(self):
        """Entry point for the client prompt."""
        _clear()
        client_name = input("Enter your name: ")
        client_password = getpass("Enter your password: ").strip()
        server_addr = input("Server address: ")
        self._connect(client_name, client_password, server_addr)
