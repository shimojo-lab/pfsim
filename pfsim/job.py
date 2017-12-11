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
        self.link_usage = defaultdict(lambda: 0)
        self.link_flows = defaultdict(lambda: 0)
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

    def _started(self, job):
        if self != job:
            return

        self.started_at = self.simulator.time

    def __repr__(self):  # pragma: no cover
        return "<Job {0} np:{1} {2}>".format(self.name, self.n_procs,
                                             self.status.name)

    @classmethod
    def from_trace(cls, path, duration=0.0, generator=None, simulator=None):
        with open(str(path), "rb") as f:
            matrix = TrafficMatrix.load(f)
        n_procs = matrix.n_procs
        name = "{0}-{1}".format(Path(path).name, cls._serial)
        cls._serial += 1

        return cls(name, n_procs, duration, matrix, generator, simulator)
