"""
Usage:
  pfsim [-v | --verbose] [-p | --parallel <num_procs>] <path/to/scenario>
  pfsim (-h | --help)

Options:
  -h --help     Show this help.
  -v --verbose  Enable verbose logging.
  -p --parallel Specify the degree of parallelism.
     --version  Show version info.
"""

import logging.config
from logging import getLogger

from docopt import docopt

from .experiment import Experiment


__VERSION__ = "1.0.0"

logger = getLogger(__name__)


def configure_logging(verbose=False):
    loglevel = "DEBUG" if verbose else "INFO"

    logging.config.dictConfig({
        "version": 1,
        "formatters": {
            "color": {
                "()": "colorlog.ColoredFormatter",
                "format": "%(log_color)s[%(levelname)s]%(reset)s [%(name)s]: "
                          "%(message)s",
                "datefmt": "%H:%M:%S"
            }
        },
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "formatter": "color",
                "stream": "ext://sys.stdout",
            }
        },
        "root": {
            "level": loglevel,
            "handlers": ["default"],
        },
        "disable_existing_loggers": False
    })


def main():
    args = docopt(__doc__, version=__VERSION__)
    configure_logging(verbose=args["--verbose"])

    logger.info("Starting pfsim %s", __VERSION__)

    experiment = Experiment(args["<path/to/scenario>"])
    if args["--parallel"]:
        num_procs = int(args["--parallel"][0])
        experiment.run_parallel(num_procs)
    else:
        experiment.run_serial()
