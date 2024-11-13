import csv
from typing import IO


def load_ids(ids_file: IO) -> list[str]:
    dpla_ids = []
    csv_reader = csv.reader(ids_file)
    for row in csv_reader:
        dpla_ids.append(row[0])
    return dpla_ids


def null_safe[T](data: dict, field_name: str, identity_element: T) -> T:
    if data is not None:
        return data.get(field_name, identity_element)
    else:
        return identity_element


def get_list(data: dict, field_name: str) -> list:
    """Null safe shortcut for getting an array from a dict."""
    return null_safe(data, field_name, [])


def get_str(data: dict, field_name: str) -> str:
    """Null safe shortcut for getting a string from a dict."""
    return null_safe(data, field_name, "")


def get_dict(data: dict, field_name: str) -> dict:
    """Null safe shortcut for getting a dict from a dict."""
    return null_safe(data, field_name, {})
