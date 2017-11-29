"""
Usage:
  pfsim [-v | --verbose] <path/to/scenario>
  pfsim (-h | --help)

Options:
  -h --help     Show this help.
  -v --verbose  Enable verbose logging.
     --version  Show version info.
"""

import logging.config

from docopt import docopt

from .experiment import Experiment


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
    args = docopt(__doc__, version="pfsim 0.1.0")
    configure_logging(verbose=args["--verbose"])

    experiment = Experiment(args["<path/to/scenario>"])
    experiment.run()
