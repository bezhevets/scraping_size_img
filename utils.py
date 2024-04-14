import functools
import logging
import sys
import time
from typing import Callable, Any


def time_counter(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        print("========Start script========")
        start = time.perf_counter()
        func(*args, **kwargs)
        duration = time.perf_counter() - start
        print(f"========Duration: {round(duration, 2)} seconds========")
        print("========Finish script========")
    return wrapper


def log_settings() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s]: %(message)s",
        handlers=[
            logging.FileHandler("log.log", mode="w"),
            logging.StreamHandler(sys.stdout),
        ],
    )

