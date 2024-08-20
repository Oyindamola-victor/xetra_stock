"""
Custom Exceptions
"""


class WrongFormatException(Exception):
    """
    WrongFormatException class

    This is an Exception that can be raised when the format type
    given as a parameter is not supported
    """


class WrongMetaFileException(Exception):
    """
    WrongMetaFileException class

    This is an Exception that can be raised when there is a
    difference in columns between the metafile columns and the new dataframe
    that needs to be combined
    """
