
from trackers.tracker import Tracker
from utilities.fs import FileSystem

class Entry():
    """
    """

    @staticmethod
    def load_data(data_in, columns = None, file_filter = None):
        """
        Load data from parquet file and filter out ids if a file filter is provided
        """
        fs = FileSystem()
        data = fs.read_parquet(data_in, cols=columns)

        if file_filter:
            exclude_ids = []
            with open(file_filter, encoding='utf-8') as f:
                exclude_ids = [line.rstrip() for line in f]
            return data.filter(lambda x: x.id in exclude_ids)

        return data

    def execute(self, tracker: Tracker, **kwargs):
        raise NotImplementedError
