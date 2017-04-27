import json
import tarfile
from collections import defaultdict
from enum import Enum
from pathlib import Path


JobStatus = Enum("JobStatus", "CREATED QUEUED RUNNING FINISHED")


class TrafficMatrix:
    _cache = {}

    def __init__(self, n_procs, coo=[]):
        self.n_procs = n_procs
        self.coo = coo

    def items(self):
        return self.coo

    def __len__(self):
        return len(self.coo)

    @property
    def density(self):
        return len(self) / self.n_procs ** 2

    @property
    def sparsity(self):
        return 1.0 - self.density

    @classmethod
    def load(cls, path):
        if path in cls._cache:
            return cls._cache[path]

        # c.f. https://www.wikiwand.com/en/Sparse_matrix
        coo = []
        n_procs = 0

        with tarfile.open(path, "r:gz") as tar:
            for member in tar.getmembers():
                if not member.isfile() or not member.name.endswith(".json"):
                    continue

                with tar.extractfile(member) as f:
                    trace = json.load(f)
                    src = trace["rank"]
                    n_procs += 1

                    for dst, tx_bytes in enumerate(trace["tx_bytes"]):
                        if tx_bytes <= 0.0:
                            continue

                        coo.append((src, dst, tx_bytes))

        # Inplace sort (src, dst, volume) tripltes by volume
        coo.sort(key=lambda triple: triple[2], reverse=True)

        matrix = cls(n_procs, coo)

        cls._cache[path] = matrix

        return matrix


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

    def _started(self, job):
        if self != job:
            return

        self.started_at = self.simulator.time

        for src, dst, traffic in self.traffic_matrix.items():
            self.simulator.schedule("job.message", job=job,
                                    src_proc=self.procs[src],
                                    dst_proc=self.procs[dst],
                                    traffic=traffic)

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
