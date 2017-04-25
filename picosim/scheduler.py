from collections import deque
from logging import getLogger
from weakref import proxy

from .job import JobStatus
from .process import Process

logger = getLogger(__name__)


class FCFSScheduler:
    def __init__(self, simulator, selector, mapper):
        self.simulator = simulator
        self.queue = deque()
        self.selector = selector
        self.mapper = mapper

        self.n_running = 0
        self.n_finished = 0

        simulator.register("job.submitted", self._job_submitted)
        simulator.register("job.finished", self._job_finished)

    @property
    def n_queued(self):
        return len(self.queue)

    def _job_submitted(self, job, hosts):
        logger.info("Job {0} subimitted at {1}".format(
            job.name, self.simulator.time))
        # If queue is not empty or hosts are occupied
        if self.queue or not self.selector.test(job, hosts):
            logger.info("Job {0} queued ({1} jobs queued)".format(
                job.name, self.n_queued))
            job.status = JobStatus.QUEUED
            self.queue.append(job)
            return

        self._run_job(job, hosts)

    def _job_finished(self, job, hosts):
        logger.info("Job {0} finished at {1}".format(
            job.name, self.simulator.time))

        self.n_running -= 1
        self.n_finished += 1
        job.status = JobStatus.FINISHED

        # Release hosts
        for host in job.hosts:
            host.allocated = False
            host.job = None
            host.procs = []

        # If queue is empty, do nothing
        if not self.queue:
            return

        job = self.queue.popleft()
        # If there are not enough available hosts
        if not self.selector.test(job, hosts):
            # Put back job to queue
            self.queue.appendleft(job)
            return

        self._run_job(job, hosts)

    def _run_job(self, job, hosts):
        logger.info("Job {0} starting at {1}".format(
            job.name, self.simulator.time))

        self.n_running += 1
        job.status = JobStatus.RUNNING

        # Select and allocate hosts
        selected_hosts = self.selector.select(job, hosts)
        for host in selected_hosts:
            host.allocated = True
            host.job = job

        job.hosts = [proxy(host) for host in selected_hosts]

        # Generate and map processs
        procs = [Process(job.name, rank) for rank in range(job.n_procs)]
        for proc, host in self.mapper.map(procs, selected_hosts).items():
            host.procs.append(proc)
            proc.host = proxy(host)

        # Associate job and processes
        job.procs = procs
        for proc in procs:
            proc.job = proxy(job)

        self.simulator.schedule("job.started", job=job)
        self.simulator.schedule_after("job.finished", job.duration,
                                      job=job, hosts=hosts)
