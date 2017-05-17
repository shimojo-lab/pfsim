from collections import defaultdict
from enum import Enum
from pathlib import Path

from .traffic_matrix import TrafficMatrix


JobStatus = Enum("JobStatus", "CREATED QUEUED RUNNING FINISHED")


class Job:
    _serial = 0

    def __init__(self, name, n_procs=1, duration=0.0, traffic_matrix=None,
                 generator=None, simulator=None):
        self.simulator = simulator
        self.name = name
        self.n_procs = n_procs
        self.duration = duration
        if traffic_matrix is None:
            traffic_matrix = TrafficMatrix(n_procs)
        self.traffic_matrix = traffic_matrix
        self.link_usage = defaultdict(lambda: defaultdict(lambda: 0))
        self.link_flows = defaultdict(lambda: defaultdict(lambda: 0))
        self.generator = generator

        self.hosts = []
        self.procs = []

        self.created_at = None
        self.started_at = None
        self.finished_at = None
        self.status = JobStatus.CREATED

        if simulator:
            self.simulator = simulator
            self.simulator.register("job.submitted", self._submitted)
            self.simulator.register("job.started", self._started)
            self.simulator.register("job.finished", self._finished)

    def _submitted(self, job):
        if self != job:
            return

        self.created_at = self.simulator.time

    def _finished(self, job):
        if self != job:
            return

        self.finished_at = self.simulator.time

    def host_traffic_adj_list(self):
        host_tm = defaultdict(lambda: 0)

        for src_rank, dst_rank, traffic in self.traffic_matrix.adj_list():
            src_host = self.procs[src_rank].host
            dst_host = self.procs[dst_rank].host

            host_tm[(src_host, dst_host)] += traffic

        host_adj_list = list(host_tm.items())
        host_adj_list.sort(key=lambda x: x[1], reverse=True)

        adj_list = []

        for (src_host, dst_host), traffic in host_adj_list:
            for src_proc in src_host.procs:
                for dst_proc in dst_host.procs:
                    src_rank = src_proc.rank
                    dst_rank = dst_proc.rank

                    if (src_rank, dst_rank) not in self.traffic_matrix.dok:
                        continue

                    traffic = self.traffic_matrix.dok[(src_rank, dst_rank)]

                    adj_list.append((src_rank, dst_rank, traffic))

        return adj_list

    def _started(self, job):
        if self != job:
            return

        self.started_at = self.simulator.time

    def __repr__(self):
        return "<Job {0} np:{1} {2}>".format(self.name, self.n_procs,
                                             self.status.name)

    @classmethod
    def from_trace(cls, path, duration=0.0, generator=None, simulator=None):
        matrix = TrafficMatrix.load(path)
        n_procs = matrix.n_procs
        name = "{0}-{1}".format(Path(path).name, cls._serial)
        cls._serial += 1

        return cls(name, n_procs, duration, matrix, generator, simulator)
