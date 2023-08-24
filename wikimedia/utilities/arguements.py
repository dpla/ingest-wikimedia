import getopt
import sys

@staticmethod
def get_args(args):
    params = {}

    try:
        opts, args = getopt.getopt(args,
                                "hi:u:o:",
                                ["partner=",
                                 "limit=",
                                 "input=",
                                 "output=",
                                 "file_filter=",
                                 "type="])
    except getopt.GetoptError:
        print(
            "run.py\n" \
            "--partner <dpla partner name>\n" \
            "--limit <bytes>\n" \
            "--input <path to parquet>\n" \
            "--output <path to save files>\n" \
            "--file_filter <ids>" \
            # TODO EVENT_TYPE
            )
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print(
                "run.py\n" \
                    "--partner <dpla partner name>\n" \
                    "--input <ingestion3 wiki ouput>\n" \
                    "--output <path to save files>\n" \
                    "--limit <total Download limit in bytes>\n" \
                    "--file_filter <Download only these DPLA ids to download>"
                    # TODO EVENT_TYPE
                    )
            sys.exit()
        elif opt in ("-p", "--partner"):
            params["partner"] = arg
        elif opt in ("-i", "--input"):
            params["input"] = arg
        elif opt in ("-o", "--output"):
            params["output"] = arg.rstrip('/')
        elif opt in ('-t', '--type'):
            params['type'] = arg
        # DOWNLOAD ONLY PARAMS
        elif opt in ("-l", "--limit"):
            params["total_limit"] = int(arg)
        elif opt in ("-f", "--file_filter"):
            params["file_filter"] = arg
    return params