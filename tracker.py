from enum import Enum

Result = Enum("Result", ["DOWNLOADED", "FAILED", "SKIPPED", "UPLOADED", "BYTES"])


class Tracker:
    def __init__(self):
        self.data = {}

    def increment(self, status: Result, amount=1) -> None:
        if status not in self.data:
            self.data[status] = 0
        self.data[status] = self.data[status] + amount

    def count(self, status: Result) -> int:
        if status not in self.data:
            return 0
        else:
            return self.data[status]

    def __str__(self) -> str:
        result = "COUNTS:\n"
        for key in self.data:
            value = self.data[key]
            result += f"{key.name}: {value}\n"
        return result
