from enum import Enum, auto
from threading import Lock


class Result(Enum):
    DOWNLOADED = auto()
    FAILED = auto()
    SKIPPED = auto()
    UPLOADED = auto()
    BYTES = auto()
    ITEM_NOT_PRESENT = auto()
    BAD_IIIF_MANIFEST = auto()
    NO_MEDIA = auto()
    BAD_IMAGE_API = auto()


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Tracker(metaclass=Singleton):
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

    def reset(self):
        with self.lock:
            for value in Result:
                self.data[value] = 0

    def __str__(self) -> str:
        result = "COUNTS:\n"
        for key in self.data:
            value = self.data[key]
            if value > 0:
                result += f"{key.name}: {value}\n"
        return result
