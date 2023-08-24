from utilities.helpers import ParquetHelper
from utilities.tracker import Tracker


class Entry():
    """
    """

    @staticmethod
    def load_data(data_in, columns = None, file_filter = None):
        """
        Load data from parquet file and filter out ids if a file filter is provided
        """
        fs = ParquetHelper()
        data = fs.read_parquet(data_in, columns=columns)
        if file_filter:
            exclude_ids = []
            with open(file_filter, encoding='utf-8') as f:
                exclude_ids = [line.rstrip() for line in f]
            data = data.filter(lambda x: x.id in exclude_ids)
        return data.head(10)

    def execute(self, tracker: Tracker, **kwargs):
        raise NotImplementedError
