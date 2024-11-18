from enum import Enum
from threading import Lock

Result = Enum("Result", ["DOWNLOADED", "FAILED", "SKIPPED", "UPLOADED", "BYTES"])


class SingletonBase:
    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__new__(cls)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Tracker(SingletonBase):
    def __init__(self):
        self.data = {}
        for value in Result:
            self.data[value] = 0
        self.lock = Lock()

    def increment(self, status: Result, amount=1) -> None:
        with self.lock:
            self.data[status] = self.data[status] + amount

    def count(self, status: Result) -> int:
        return self.data[status]

    def __str__(self) -> str:
        result = "COUNTS:\n"
        for key in self.data:
            value = self.data[key]
            result += f"{key.name}: {value}\n"
        return result
