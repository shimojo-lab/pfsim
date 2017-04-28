import logging.config


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
