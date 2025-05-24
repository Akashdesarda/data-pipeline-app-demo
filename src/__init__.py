import logging

from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("data-pipeline")
logger.setLevel(logging.DEBUG)

# Getting rich console handler for better logging output
ch = RichHandler(console=Console(width=120))
formatter = logging.Formatter("%(message)s", datefmt="%d-%b-%y %H:%M:%S")
ch.setFormatter(formatter)
# adding the handler to the logger
logger.addHandler(ch)
