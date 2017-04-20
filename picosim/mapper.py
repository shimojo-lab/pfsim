from logging import getLogger

from .process import Process

logger = getLogger(__name__)


class SimpleMapper:
    def __init__(self, simulator):
        self.simulator = simulator
        simulator.register("job allocated", self.map)

    def map(self, job, allocated_hosts):
        procs_togo = job.n_procs

        rank = 0
        for host in allocated_hosts:
            for i in range(host.capacity):
                if procs_togo <= 0:
                    break

                process = Process(job.name, rank, host.name)
                host.running_procs.append(process)

                procs_togo -= 1
                rank += 1

        self.simulator.schedule("job launched")
