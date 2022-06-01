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

    def connect(self):
        raise NotImplementedError()

    def disconnect(self):
        raise NotImplementedError()

    def upload_file(self, file_path: str):
        raise NotImplementedError()

    def download_file(self, folder_path: str):
        raise NotImplementedError()

    def delete_file(self, file_name: str):
        raise NotImplementedError()

    def list_files(self):
        raise NotImplementedError()
