import getopt
import sys

def get_download_args(args):
    """
    """
    params = {}
    try:
        opts, args = getopt.getopt(args,
                                "hi:u:o:",
                                ["partner=",
                                    "limit=",
                                    "input=",
                                    "output=",
                                    "file_filter="])
    except getopt.GetoptError:
        print(
            "downloader.py\n" \
            "--partner <dpla partner name>\n" \
            "--limit <bytes>\n" \
            "--input <path to parquet>\n" \
            "--output <path to save files>\n" \
            "--file_filter <ids>" \
            )
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print(
                "downloader.py\n" \
                    "--partner <dpla partner name>\n" \
                    "--limit <total limit in bytes>\n" \
                    "--input <path to wikimedia parquet file>\n" \
                    "--output <path to save files>\n" \
                    "--file_filter <file that specifies DPLA ids to download>"
                    )
            sys.exit()
        elif opt in ("-p", "--partner"):
            # self.partner_name = arg
            params["partner_name"] = arg
        elif opt in ("-l", "--limit"):
            # self.total_limit = int(arg)
            params["total_limit"] = int(arg)
        elif opt in ("-i", "--input"):
            # self.input_data = arg
            params["input_data"] = arg
        elif opt in ("-o", "--output"):
            # self.output_base = arg.rstrip('/')
            params["output_base"] = arg.rstrip('/')
        elif opt in ("-f", "--file_filter"):
            # self.file_filter = arg
            params["file_filter"] = arg

    return params



# TODO add upload param to this