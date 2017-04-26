from logging import getLogger
from math import inf

logger = getLogger(__name__)


class TimeSeries:
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
        w = (time - self.current_time) / time
        self.mean = self.mean * (1.0 - w) + self.current_value * w
        self.count += 1

        if self.store:
            self.data.append((self.current_time, self.current_value))
        self.current_time = time
        self.current_value = value

    def report(self):
        logger.info("{0:=^80}".format(" " + self.name + "  "))
        logger.info("Max: {0}".format(self.max))
        logger.info("Min: {0}".format(self.min))
        logger.info("Mean: {0}".format(self.mean))
        logger.info("Count: {0}".format(self.count))


class SchedulerMetricsCollector:
    def __init__(self, simulator, cluster):
        self.simulator = simulator
        self.cluster = cluster

        self.n_queued = TimeSeries("Scheduler Queue Length")
        self.n_running = TimeSeries("Number of Running Jobs")
        self.n_finished = TimeSeries("Number of Finished Jobs")

        simulator.register("job.submitted", self._collect, prio=-inf)
        simulator.register("job.started", self._collect, prio=-inf)
        simulator.register("job.finished", self._collect, prio=-inf)

    def _collect(self, **kwargs):
        time = self.simulator.time

        self.n_queued.add(time, self.cluster.scheduler.n_queued)
        self.n_running.add(time, self.cluster.scheduler.n_running)
        self.n_finished.add(time, self.cluster.scheduler.n_finished)

    def report(self):
        self.n_queued.report()
        self.n_running.report()
        self.n_finished.report()


class InterconnectMetricsCollector:
    def __init__(self, simulator, cluster):
        self.simulator = simulator
        self.cluster = cluster

        self.max_congestion = TimeSeries("Maximum Congestion")
        self.max_flows = TimeSeries("Maximum Number of Flows")

        simulator.register("job.message", self._collect, prio=-inf)

    def _collect(self, **kwargs):
        time = self.simulator.time

        max_congestion = -inf
        max_flows = -inf

        for u, v, attrs in self.cluster.graph.edges_iter(data=True):
            congestion = attrs["traffic"] / attrs["capacity"]
            max_congestion = max(congestion, max_congestion)
            max_flows = max(attrs["flows"], max_flows)

        self.max_congestion.add(time, max_congestion)
        self.max_flows.add(time, max_flows)

    def report(self):
        self.max_congestion.report()
        self.max_flows.report()
