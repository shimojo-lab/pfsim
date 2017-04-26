from logging import getLogger
from math import inf, nan

logger = getLogger(__name__)


class Samples:
    def __init__(self, name, store=True):
        self.name = name
        self.store = store
        self.values = []

        self.count = 0
        self.max = -inf
        self.min = inf
        self.mean = 0.0
        self.m2 = 0.0

    def add(self, value):
        self.max = max(self.max, value)
        self.min = min(self.min, value)
        self.count += 1

        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

        if self.store:
            self.values.append(value)

    @property
    def variance(self):
        if self.count < 2:
            return nan

        return self.m2 / (self.count - 1)

    def report(self):
        logger.info("{0:=^80}".format(" " + self.name + "  "))
        logger.info("Max:       {0}".format(self.max))
        logger.info("Min:       {0}".format(self.min))
        logger.info("Mean:      {0}".format(self.mean))
        logger.info("Count:     {0}".format(self.count))
        logger.info("Variance:  {0}".format(self.variance))


class TimeSeriesSamples(Samples):
    def __init__(self, name, store=True):
        super().__init__(name, store)
        self.times = []
        self.current_time = None
        self.current_value = None

        self.t_mean = 0.0
        self.t_m2 = 0.0

    def add(self, time, value):
        if self.current_time is None:
            self.current_time = time
            self.current_value = value
            return
        if time <= self.current_time:
            self.current_value = value
            return

        delta = self.current_value - self.t_mean
        w = (time - self.current_time) / time
        self.t_mean = self.t_mean * (1.0 - w) + self.current_value * w
        delta2 = self.current_value - self.t_mean
        self.t_m2 += (time - self.current_time) * delta * delta2

        if self.store:
            self.times.append(self.current_time)

        super().add(self.current_value)

        self.current_time = time
        self.current_value = value

    @property
    def t_variance(self):
        if self.count < 2:
            return nan

        return self.t_m2 / self.current_time

    def report(self):
        logger.info("{0:=^80}".format(" " + self.name + "  "))
        logger.info("Max:       {0}".format(self.max))
        logger.info("Min:       {0}".format(self.min))
        logger.info("Mean:      {0}".format(self.t_mean))
        logger.info("Count:     {0}".format(self.count))
        logger.info("Variance:  {0}".format(self.t_variance))
