import getopt
import sys

@staticmethod
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
                    "--input <ingestion3 wiki ouput>\n" \
                    "--output <path to save files>\n" \
                    "--file_filter <file that specifies DPLA ids to download>"
                    )
            sys.exit()
        elif opt in ("-p", "--partner"):
            params["partner_name"] = arg
        elif opt in ("-l", "--limit"):
            params["total_limit"] = int(arg)
        elif opt in ("-i", "--input"):
            params["input_data"] = arg
        elif opt in ("-o", "--output"):
            params["output_base"] = arg.rstrip('/')
        elif opt in ("-f", "--file_filter"):
            params["file_filter"] = arg
    return params


@staticmethod
def get_upload_args(args):
    params = {}
    try:
        opts, args = getopt.getopt(sys.argv[1:],
                                "hi:u:o:",
                                ["input=",
                                 "partner="])
    except getopt.GetoptError:
        print('upload-entry.py --partner <dpla partner abbreviation> --input <path to parquet>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print(
                'upload-entry.py --partner <DPLA hub abbreviation> --input <path to parquet>')
            sys.exit()
        elif opt in ("-i", "--input"):
            params['input'] = arg
        elif opt in ("-p", "--partner"):
            params['partner_name'] = arg
    return params