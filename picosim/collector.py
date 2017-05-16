from logging import getLogger
from math import inf, sqrt
from pathlib import Path

from .statistics import Samples, TimeSeriesSamples

logger = getLogger(__name__)


class SchedulerMetricsCollector:
    def __init__(self, simulator, cluster):
        self.simulator = simulator
        self.cluster = cluster

        self.n_queued = TimeSeriesSamples("Number of Waiting Jobs")
        self.n_system = TimeSeriesSamples("Number of Jobs in System")
        self.n_running = TimeSeriesSamples("Number of Running Jobs")
        self.n_finished = TimeSeriesSamples("Number of Finished Jobs")
        self.wait_time = Samples("Job Wait Time")
        self.response_time = Samples("Job Response Time")

        self.metrics = [
            self.n_queued,
            self.n_system,
            self.n_running,
            self.n_finished,
            self.wait_time,
            self.response_time
        ]

        simulator.register("job.submitted", self._collect, prio=-inf)
        simulator.register("job.started", self._collect, prio=-inf)
        simulator.register("job.finished", self._collect, prio=-inf)
        simulator.register("job.finished", self._finished, prio=-inf)

    def _finished(self, job, **kwargs):
        self.wait_time.add(job.started_at - job.created_at)
        self.response_time.add(job.finished_at - job.created_at)

    def _collect(self, **kwargs):
        time = self.simulator.time

        self.n_queued.add(time, self.cluster.scheduler.n_queued)
        self.n_system.add(time, self.cluster.scheduler.n_queued +
                          self.cluster.scheduler.n_running)
        self.n_running.add(time, self.cluster.scheduler.n_running)
        self.n_finished.add(time, self.cluster.scheduler.n_finished)

    def report(self):
        for metric in self.metrics:
            metric.report()

    def plot(self, output_dir):
        for metric in self.metrics:
            path = Path(output_dir) / Path(metric.name).with_suffix(".png")
            metric.plot(str(path))


class InterconnectMetricsCollector:
    def __init__(self, simulator, cluster):
        self.simulator = simulator
        self.cluster = cluster

        self.max_congestion = TimeSeriesSamples("Maximum Congestion")
        self.stddev_congestion = TimeSeriesSamples("Congestion Std Deviation")
        self.max_flows = TimeSeriesSamples("Maximum Number of Flows")

        self.metrics = [
            self.max_congestion,
            self.stddev_congestion,
            self.max_flows
        ]

        simulator.register("job.message", self._collect, prio=-inf)

    def _collect(self, **kwargs):
        time = self.simulator.time

        n = 0
        mean_congestion = 0.0
        m2_congestion = 0.0
        max_congestion = -inf
        max_flows = -inf

        for u, v, attrs in self.cluster.graph.edges_iter(data=True):
            if u in self.cluster.hosts or v in self.cluster.hosts:
                continue

            congestion = attrs["traffic"] / attrs["capacity"]

            max_congestion = max(congestion, max_congestion)
            max_flows = max(attrs["flows"], max_flows)

            n += 1
            delta = congestion - mean_congestion
            mean_congestion += delta / n
            delta2 = congestion - mean_congestion
            m2_congestion += delta * delta2

        stddev_congestion = sqrt(m2_congestion / (n - 1))

        self.max_congestion.add(time, max_congestion)
        self.stddev_congestion.add(time, stddev_congestion)
        self.max_flows.add(time, max_flows)

    def report(self):
        for metric in self.metrics:
            metric.report()

    def plot(self, output_dir):
        for metric in self.metrics:
            path = Path(output_dir) / Path(metric.name).with_suffix(".png")
            metric.plot(str(path))
