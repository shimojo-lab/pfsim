from logging import getLogger
from math import inf

logger = getLogger(__name__)


class TimeSeries:
    def __init__(self):
        # List of (time, value)
        self.data = []
        self.last_time = None

        self.max = -inf
        self.min = inf
        self.mean = 0.0

    def add(self, time, value):
        if self.last_time is not None and time < self.last_time:
            raise RuntimeError("Cannot add older values")
        elif self.last_time is not None and time == self.last_time:
            self.data[-1] = (time, value)
            return

        self.max = max(self.max, value)
        self.min = min(self.min, value)
        if self.last_time is not None:
            w = (time - self.last_time) / time
            self.mean = self.mean * (1 - w) + self.data[-1][1] * w

        self.data.append((time, value))
        self.last_time = time


class SchedulerMetricsCollector:
    def __init__(self, simulator, cluster):
        self.simulator = simulator
        self.cluster = cluster

        self.n_queued = TimeSeries()

        simulator.register("job.submitted", self._collect)
        simulator.register("job.started", self._collect)
        simulator.register("job.finished", self._collect)

    def _collect(self, **kwargs):
        time = self.simulator.time

        self.n_queued.add(time, self.cluster.scheduler.n_queued)

    def report(self):
        for time, value in self.n_queued.data:
            print(time, value)

        print("Max:", self.n_queued.max)
        print("Min:", self.n_queued.min)
        print("Mean:", self.n_queued.mean)
