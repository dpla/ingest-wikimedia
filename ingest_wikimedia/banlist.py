from pathlib import Path

BANLIST_FILE_NAME = "dpla-id-banlist.txt"


class Banlist:
    def __init__(self) -> None:
        banlist_path = Path(__file__).parent.parent / BANLIST_FILE_NAME
        with open(banlist_path, "r") as file:
            self.dpla_id_banlist = set([line.rstrip() for line in file])

    def is_banned(self, dpla_id: str) -> bool:
        """
        Checks if the given DPLA ID is in the banlist.
        """
        return dpla_id in self.dpla_id_banlist
