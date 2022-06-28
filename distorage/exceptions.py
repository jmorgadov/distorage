"""
Contains the exceptions handled by Distorage.
"""


class ServiceConnectionError(Exception):
    """
    Raised when a connection to a service could not be established.
    """

    def __init__(self, msg: str):
        super().__init__(f"Could not connect to the service: {msg}.")


class DHTOperationError(Exception):
    """
    Raised when a DHT operation could not be performed.
    """

    def __init__(self, msg: str):
        super().__init__(f"Could not perform the DHT operation: {msg}.")
