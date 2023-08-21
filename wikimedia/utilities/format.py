@staticmethod
def sizeof_fmt(num, suffix='B'):
    """
    Convert bytes to human readable format

    :param num: number of bytes
    :param suffix: suffix to append to number
    :return: human readable string
    """
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

@staticmethod
def number_fmt(num):
    """
    Convert number to human readable format

    :param num: number
    :return: human readable string
    """
    return "{:,}".format(num)