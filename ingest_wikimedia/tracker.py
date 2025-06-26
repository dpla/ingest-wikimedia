from enum import Enum, auto


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
    RETIRED = auto()


class Tracker:
    def __init__(self):
        self.data = {}
        for value in Result:
            self.data[value] = 0

    def increment(self, status: Result, amount=1) -> None:
        self.data[status] = self.data[status] + amount

    def count(self, status: Result) -> int:
        return self.data[status]

    def reset(self):
        for value in Result:
            self.data[value] = 0

    def __str__(self) -> str:
        result = "COUNTS:\n"
        for key in self.data:
            value = self.data[key]
            if value > 0:
                result += f"{key.name}: {value}\n"
        return result
