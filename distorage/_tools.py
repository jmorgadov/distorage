"""
Contains some useful functions.
"""

import asyncio
from typing import Callable


def repeat_async(time_interval: float) -> Callable:
    """
    Repeats a function every `time_interval` seconds asynchronously.

    Parameters
    ----------
    time_interval : float
        The time interval between each call of the function.

    Returns
    -------
    Callable
        A decorator that repeats a function every `time_interval` seconds.
    """

    def decorator(func):
        async def wrapper(*args, **kwargs):
            while True:
                func(*args, **kwargs)
                await asyncio.sleep(time_interval)

        return wrapper

    return decorator
