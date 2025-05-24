class DataLoaderError(Exception):
    """
    Exception raised when there is an error loading data.

    Parameters
    ----------
    message : str
        The error message to be displayed when the exception is raised.
    """

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class DataWriterError(Exception):
    """
    Exception raised when there is an error writing data.

    Parameters
    ----------
    message : str
        The error message to be displayed when the exception is raised.
    """

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
