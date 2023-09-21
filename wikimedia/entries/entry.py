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
        include_ids = ['09ca347b4c8883fa86dc17f974a561f5']
        if file_filter:
            include_ids = []
            with open(file_filter, encoding='utf-8') as f:
                include_ids = [line.rstrip() for line in f]
            data = data.filter(lambda x: x.id in include_ids)
        import numpy as np
        import pandas as pd
        from IPython.display import display
        display(data)
        filter = data["dpla_id"]=="09ca347b4c8883fa86dc17f974a561f5"
        filtered = data.where(filter).dropna().astype({"size":"int", "page":"int"})
        display(filtered)
        return filtered

    def execute(self, tracker: Tracker, **kwargs):
        raise NotImplementedError
