import json
import tarfile
from enum import Enum
from pathlib import Path


JobStatus = Enum("JobStatus", "CREATED QUEUED RUNNING FINISHED")


class Job:
    def __init__(self, name, n_procs=1, duration=0.0, traffic_matrix=None,
                 simulator=None):
        self.simulator = simulator
        self.name = name
        self.n_procs = n_procs
        self.duration = duration
        if traffic_matrix is None:
            traffic_matrix = []
        self.traffic_matrix = traffic_matrix

        self.hosts = []
        self.procs = []

        self.status = JobStatus.CREATED

        if simulator:
            self.simulator = simulator
            self.simulator.register("job.started", self.started)

    def started(self, job):
        if self != job:
            return

        for src, row in enumerate(self.traffic_matrix):
            for dst, traffic in enumerate(row):
                if traffic <= 0.0:
                    continue

                self.simulator.schedule("job.communicate",
                                        src_proc=self.procs[src],
                                        dst_proc=self.procs[dst],
                                        traffic=traffic)

    def __repr__(self):
        return "<Job {0} np:{1} {2}>".format(self.name, self.n_procs,
                                             self.status.name)

    @classmethod
    def from_trace(cls, path, duration=0.0, simulator=None):
        tmp = {}

        with tarfile.open(path, "r:gz") as tar:
            for member in tar.getmembers():
                if not member.isfile() or not member.name.endswith(".json"):
                    continue

                with tar.extractfile(member) as f:
                    trace = json.load(f)
                    rank = trace["rank"]
                    tx_bytes = trace["tx_bytes"]

                    tmp[rank] = tx_bytes

        n_procs = len(tmp)

        matrix = [[0 for i in range(n_procs)] for y in range(n_procs)]

        for src, vec in tmp.items():
            for dst, traffic in enumerate(vec):
                matrix[src][dst] = traffic

        name = Path(path).name
        return cls(name, n_procs, duration, matrix, simulator)
