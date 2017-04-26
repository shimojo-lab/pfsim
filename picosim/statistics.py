from logging import getLogger
from math import inf, sqrt

logger = getLogger(__name__)


class TimedSamples:
    def __init__(self, name, store=True):
        self.name = name
        self.store = store
        self.data = []
        self.current_time = None
        self.current_value = None

        # Statistics
        self.count = 0
        self.max = -inf
        self.min = inf
        self.mean = 0.0
        self.m2 = 0.0
        self.tmp = 0.0

    def add(self, time, value):
        if self.current_time is None:
            self.current_time = time
            self.current_value = value
            return
        if time <= self.current_time:
            self.current_value = value
            return

        # Update statistics
        self.max = max(self.max, self.current_value)
        self.min = min(self.min, self.current_value)
        self.count += 1

        delta = self.current_value - self.mean
        w = (time - self.current_time) / time
        self.mean = self.mean * (1.0 - w) + self.current_value * w
        delta2 = self.current_value - self.mean
        self.m2 += (time - self.current_time) * delta * delta2

        self.tmp += self.current_value

        if self.store:
            self.data.append((self.current_time, self.current_value))
        self.current_time = time
        self.current_value = value

    @property
    def variance(self):
        return self.m2 / self.current_time

    @property
    def sd(self):
        return sqrt(self.variance)

    def report(self):
        logger.info("{0:=^80}".format(" " + self.name + "  "))
        logger.info("Max:       {0}".format(self.max))
        logger.info("Min:       {0}".format(self.min))
        logger.info("Mean:      {0}".format(self.mean))
        logger.info("Count:     {0}".format(self.count))
        logger.info("Variance:  {0}".format(self.variance))
        logger.info("SD:        {0}".format(self.sd))
        logger.info("Tmp:       {0}".format(self.tmp / self.count))
