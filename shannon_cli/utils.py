from asyncio import Task, create_task
from collections import OrderedDict
from contextlib import AbstractAsyncContextManager
from datetime import date, datetime, timedelta
from time import time
from typing import Callable, NamedTuple


class TaskCacheItem(NamedTuple):
    timestamp: float
    task: Task


class TaskCache(AbstractAsyncContextManager):
    """
    A context manager that caches tasks/coroutines for a given TTL (time to live)
    """

    def __init__(self, ttl_seconds: int = 30):  # Default TTL is 30 seconds
        self.ttl = ttl_seconds
        self.cache = {}

    def log(self, *args, **kwargs):
        print(f"[> {self.__class__.__name__}]", *args, **kwargs)

    async def __aenter__(self) -> "TaskCache":
        self.log(f"Context manager started with {len(self.cache)} tasks.")
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> bool | None:
        # Clear expired tasks
        current_time = time()
        keys_to_delete = [
            key
            for key, task_cache_item in self.cache.items()
            if current_time - task_cache_item.timestamp >= self.ttl
        ]
        for key in keys_to_delete:
            self.log(f"Clearing expiring task {key}")
            del self.cache[key]

    async def get_or_create_task(self, key: str | int, coro_func: Callable):
        current_time = time()

        # Check if the key exists and hasn't expired
        if task_cache_item := self.cache.get(key):
            if current_time - task_cache_item.timestamp < self.ttl:
                self.log(f"Cache HIT for {key}")
                return await task_cache_item.task

        # If we reach here, either the key didn't exist or it expired
        self.log(f"Cache MISS for {key}")

        self.cache[key] = TaskCacheItem(
            task=create_task(coro_func()),
            timestamp=current_time,
        )
        return await self.cache[key].task


class LRU(OrderedDict):
    def __init__(self, maxsize, *args, **kwargs):
        self.maxsize = maxsize
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if self.maxsize and len(self) > self.maxsize:
            oldest = next(iter(self))
            del self[oldest]


class AsyncTTL:
    class _TTL(LRU):
        def __init__(self, time_to_live: int, maxsize: int):
            super().__init__(maxsize=maxsize)
            self.time_to_live = timedelta(seconds=time_to_live)
            self.maxsize = maxsize

        def __contains__(self, key):
            if key not in self.keys():
                return False
            else:
                key_expiration = super().__getitem__(key)[1]
                if key_expiration < datetime.now():
                    del self[key]
                    return False
                else:
                    return True

        def __getitem__(self, key):
            value = super().__getitem__(key)[0]
            return value

        def __setitem__(self, key, value):
            ttl_value = datetime.now() + self.time_to_live
            super().__setitem__(key, (value, ttl_value))

    def __init__(self, time_to_live=60, maxsize=1024, disable_logs=False):
        """
        :param maxsize: Use maxsize as None for unlimited size cache
        """
        self.ttl = self._TTL(time_to_live=time_to_live, maxsize=maxsize)
        self.disable_logs = disable_logs

    def log(self, *args, **kwargs):
        if not self.disable_logs:
            print(f"[> {self.__class__.__name__}]", *args, **kwargs)

    def __call__(self, func):
        async def wrapper(*args, **kwargs):
            kwargs_list = sorted([kwarg for kwarg in kwargs if isinstance(kwarg, str)])
            args_list = sorted([arg for arg in args if isinstance(arg, str)])
            key = hash("-".join(args_list) + "-".join(kwargs_list))
            if key in self.ttl:
                self.log(f"Cache HIT for {key}")
                val = self.ttl[key]
            else:
                self.log(f"Cache MISS for {key}")
                self.ttl[key] = await func(*args, **kwargs)
                val = self.ttl[key]

            return val

        wrapper.__name__ += func.__name__

        return wrapper


def from_timestamp_to_date(timestamp: float) -> date:
    dt = datetime.fromtimestamp(timestamp)
    return dt.date()
