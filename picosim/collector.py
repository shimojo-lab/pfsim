from logging import getLogger
from math import inf

logger = getLogger(__name__)


class TimeSeries:
    def __init__(self):
        # List of (time, value)
        self.data = []
        self.current_time = None
        self.current_value = None

        self.max = -inf
        self.min = inf
        self.mean = 0.0

    def add(self, time, value):
        if self.current_time is None:
            self.current_time = time
            self.current_value = value
            return
        if time <= self.current_time:
            self.current_value = value
            return

        self.max = max(self.max, self.current_value)
        self.min = min(self.min, self.current_value)
        w = (time - self.current_time) / time
        self.mean = self.mean * (1.0 - w) + self.current_value * w

        self.data.append((self.current_time, self.current_value))
        self.current_time = time
        self.current_value = value


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
