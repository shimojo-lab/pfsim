from .experiment import Experiment
from .util import configure_logging


def main():
    configure_logging()

    experiment = Experiment.from_yaml("test.yml")
    experiment.run()
