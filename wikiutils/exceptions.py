"""
This module contains all the exceptions used in the wikiutils package.
"""

class UploadException(Exception):
    """
    Base class for exceptions in this module.
    """
    def __init__(self, message):
        super().__init__(message)

class DownloadException(Exception):
    """
    Base class for exceptions in this module.
    """
    def __init__(self, message):
        super().__init__(message)

class WikiException(Exception):
    """
    Base class for exceptions in this module.
    """
    def __init__(self, message):
        super().__init__(message)

class IIIFException(Exception):
    """
    Base class for exceptions in this module.
    """
    def __init__(self, message):
        super().__init__(message)