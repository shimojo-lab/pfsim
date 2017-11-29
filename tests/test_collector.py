from unittest.mock import MagicMock

import networkx as nx

from pfsim.collector import InterconnectMetricsCollector
from pfsim.collector import SchedulerMetricsCollector
from pfsim.host import Host
from pfsim.simulator import Simulator
from pfsim.switch import Switch


class TestSchedulerMetricsCollector:
    def setup(self):
        self.sim = Simulator()
        self.cluster = MagicMock()
        self.collector = SchedulerMetricsCollector(self.sim, self.cluster)

    def test_collect(self):
        self.cluster.scheduler.n_queued = 1
        self.cluster.scheduler.n_running = 0
        self.cluster.scheduler.n_finished = 0

        self.sim.schedule("job.submitted", 0.0)
        self.sim.run()

        self.cluster.scheduler.n_queued = 0
        self.cluster.scheduler.n_running = 1
        self.cluster.scheduler.n_finished = 0

        self.sim.schedule("job.started", 1.0)
        self.sim.run()

        self.cluster.scheduler.n_queued = 1
        self.cluster.scheduler.n_running = 1
        self.cluster.scheduler.n_finished = 0

        self.sim.schedule("job.submitted", 2.0)
        self.sim.run()

        self.cluster.scheduler.n_queued = 0
        self.cluster.scheduler.n_running = 1
        self.cluster.scheduler.n_finished = 1

        job1 = MagicMock()
        job1.created_at = 0.0
        job1.started_at = 1.0
        job1.finished_at = 3.0

        self.sim.schedule("job.finished", 3.0, job=job1)
        self.sim.run()

        self.cluster.scheduler.n_queued = 0
        self.cluster.scheduler.n_running = 0
        self.cluster.scheduler.n_finished = 2

        job2 = MagicMock()
        job2.created_at = 2.0
        job2.started_at = 3.0
        job2.finished_at = 4.0

        self.sim.schedule("job.finished", 4.0, job=job2)
        self.sim.run()

        self.cluster.scheduler.n_queued = 0
        self.cluster.scheduler.n_running = 0
        self.cluster.scheduler.n_finished = 0

        self.sim.schedule("job.submitted", 5.0)
        self.sim.run()

        assert self.collector.n_queued.times == [0.0, 1.0, 2.0, 3.0, 4.0]
        assert self.collector.n_queued.values == [1, 0, 1, 0, 0]

        assert self.collector.n_system.times == [0.0, 1.0, 2.0, 3.0, 4.0]
        assert self.collector.n_system.values == [1, 1, 2, 1, 0]

        assert self.collector.n_running.times == [0.0, 1.0, 2.0, 3.0, 4.0]
        assert self.collector.n_running.values == [0, 1, 1, 1, 0]

        assert self.collector.n_finished.times == [0.0, 1.0, 2.0, 3.0, 4.0]
        assert self.collector.n_finished.values == [0, 0, 0, 1, 2]

        assert self.collector.wait_time.values == [1, 1]
        assert self.collector.response_time.values == [3, 2]


class TestInterconnectMetricsCollector:
    def setup(self):
        self.sim = Simulator()

        self.h1 = Host("h1")
        self.h2 = Host("h2")
        self.s1 = Switch("s1")
        self.s2 = Switch("s2")

        self.cluster = MagicMock()
        self.cluster.hosts = [self.h1, self.h2]

        self.g = nx.DiGraph()
        self.g.add_path([self.h1, self.s1, self.s2, self.h2])
        self.g[self.h1][self.s1]["capacity"] = 1
        self.g[self.s1][self.s2]["capacity"] = 2
        self.g[self.s2][self.h2]["capacity"] = 1
        self.cluster.graph = self.g

        self.collector = InterconnectMetricsCollector(self.sim, self.cluster)

    def test_collect(self):
        self.g[self.h1][self.s1]["traffic"] = 1
        self.g[self.s1][self.s2]["traffic"] = 1
        self.g[self.s2][self.h2]["traffic"] = 1

        self.g[self.h1][self.s1]["flows"] = 10
        self.g[self.s1][self.s2]["flows"] = 10
        self.g[self.s2][self.h2]["flows"] = 10

        self.sim.schedule("job.started", 0.0)
        self.sim.run()

        self.g[self.h1][self.s1]["traffic"] = 0
        self.g[self.s1][self.s2]["traffic"] = 0
        self.g[self.s2][self.h2]["traffic"] = 0

        self.g[self.h1][self.s1]["flows"] = 0
        self.g[self.s1][self.s2]["flows"] = 0
        self.g[self.s2][self.h2]["flows"] = 0

        self.sim.schedule("job.finished", 1.0)
        self.sim.run()

        assert self.collector.max_congestion[0.0] == 0.5
        assert self.collector.max_flows[0.0] == 10
