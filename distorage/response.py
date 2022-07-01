"""
A generic response.
"""

from typing import Tuple, TypeVar

T = TypeVar("T")

Response = Tuple[T, bool, str]
VoidResponse = Tuple[None, bool, str]


def new_response(data: T, success: bool = True, msg: str = "") -> Response[T]:
    """
    A generic response.
    """
    return data, success, msg


def new_void_response(success: bool = True, msg: str = "") -> VoidResponse:
    """
    A generic void response.
    """
    return None, success, msg


def new_error_response(msg: str = "") -> VoidResponse:
    """
    A generic error response.
    """
    return None, False, msg
