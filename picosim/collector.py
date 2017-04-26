from logging import getLogger

from .statistics import TimedSamples

from math import inf

logger = getLogger(__name__)


class SchedulerMetricsCollector:
    def __init__(self, simulator, cluster):
        self.simulator = simulator
        self.cluster = cluster

        self.n_queued = TimedSamples("Number of Waiting Jobs")
        self.n_system = TimedSamples("Number of Jobs in System")
        self.n_running = TimedSamples("Number of Running Jobs")
        self.n_finished = TimedSamples("Number of Finished Jobs")
        self.wait_time = TimedSamples("Job Wait Time")

        simulator.register("job.submitted", self._collect, prio=-inf)
        simulator.register("job.started", self._collect, prio=-inf)
        simulator.register("job.finished", self._collect, prio=-inf)
        simulator.register("job.finished", self._finished, prio=-inf)

    def _finished(self, job, **kwargs):
        wait_time = job.started_at - job.created_at
        self.wait_time.add(self.simulator.time, wait_time)

    def _collect(self, **kwargs):
        time = self.simulator.time

        self.n_queued.add(time, self.cluster.scheduler.n_queued)
        self.n_system.add(time, self.cluster.scheduler.n_queued +
                          self.cluster.scheduler.n_running)
        self.n_running.add(time, self.cluster.scheduler.n_running)
        self.n_finished.add(time, self.cluster.scheduler.n_finished)

    def report(self):
        self.n_queued.report()
        self.n_system.report()
        self.n_running.report()
        self.n_finished.report()
        self.wait_time.report()


class InterconnectMetricsCollector:
    def __init__(self, simulator, cluster):
        self.simulator = simulator
        self.cluster = cluster

        self.max_congestion = TimedSamples("Maximum Congestion")
        self.max_flows = TimedSamples("Maximum Number of Flows")

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
