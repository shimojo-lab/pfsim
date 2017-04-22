from enum import Enum


JobStatus = Enum("JobStatus", "CREATED QUEUED RUNNING FINISHED")


class Job:
    def __init__(self, name, n_procs=0, duration=0.0, traffic_matrix=[]):
        self.name = name
        self.n_procs = n_procs
        self.traffic_matrix = traffic_matrix
        self.duration = duration

        self.hosts = []
        self.procs = []

        self.status = JobStatus.CREATED

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.name == other.name

    def __repr__(self):
        return "<Job {0} np:{1} {2}>".format(self.name, self.n_procs,
                                             self.status.name)
