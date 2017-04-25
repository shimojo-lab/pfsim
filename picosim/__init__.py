"""
Usage:
  picosim [-v | --verbose] <path/to/scenario>
  picosim (-h | --help)

Options:
  -h --help     Show this help.
  -v --verbose  Enable verbose logging.
     --version  Show version info.
"""

from docopt import docopt

from .experiment import Experiment
from .util import configure_logging


def main():
    args = docopt(__doc__, version="picosim 0.1.0")
    configure_logging(verbose=args["--verbose"])

    experiment = Experiment.from_yaml(args["<path/to/scenario>"])
    experiment.run()
