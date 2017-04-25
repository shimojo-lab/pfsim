from .host import Host
from .host_selector import LinearHostSelector
from .job import Job, JobStatus
from .process_mapper import LinearProcessMapper
from .scheduler import FCFSScheduler
from .simulator import Simulator


class TestFCFSScheduler:
    def setup(self):
        self.hosts = [Host("h" + str(i), capacity=8) for i in range(8)]

        self.job1 = Job("j1", n_procs=32, duration=5.0)
        self.job2 = Job("j2", n_procs=16, duration=5.0)
        self.job3 = Job("j3", n_procs=24, duration=5.0)
        self.job4 = Job("j4", n_procs=64, duration=5.0)
        self.job5 = Job("j5", n_procs=64, duration=5.0)
        self.job6 = Job("j6", n_procs=64, duration=5.0)

        self.simulator = Simulator()
        self.scheduler = FCFSScheduler(self.simulator, LinearHostSelector(),
                                       LinearProcessMapper())

    def test_single_job(self):
        self.simulator.schedule("job.submitted", job=self.job1,
                                hosts=self.hosts)

        assert self.job1.status == JobStatus.CREATED

        self.simulator.run_until(1.0)

        assert self.job1.status == JobStatus.RUNNING

        # Allocated hosts should be associated to job
        assert all([h.job == self.job1 for h in self.hosts if h.allocated])
        # Job should be associated to procs
        assert len(self.job1.procs) == self.job1.n_procs
        # Procs should be associated to host
        assert all([proc.host for proc in self.job1.procs])
        # Procs should be associated to job
        assert all([proc.job == self.job1 for proc in self.job1.procs])

        # Sum of allocated PEs should be larger or equal than requested procs
        assert sum([h.capacity for h in self.hosts if h.allocated]) \
            >= self.job1.n_procs
        # Sum of launched procs should be equal to requested procs
        assert len(self.job1.procs) == self.job1.n_procs

        self.simulator.run()

        assert self.job1.status == JobStatus.FINISHED
        # Hosts should be released
        assert all([h.job is None and not h.allocated for h in self.hosts])

    def test_multiple_job(self):
        self.simulator.schedule("job.submitted", 1.0, job=self.job1,
                                hosts=self.hosts)
        self.simulator.schedule("job.submitted", 2.0, job=self.job2,
                                hosts=self.hosts)

        self.simulator.run_until(0.5)
        assert self.job1.status == JobStatus.CREATED

        self.simulator.run_until(1.5)
        assert self.job1.status == JobStatus.RUNNING

        self.simulator.run_until(2.5)
        assert self.job1.status == JobStatus.RUNNING
        assert self.job2.status == JobStatus.RUNNING

    def test_queueing(self):
        for host in self.hosts:
            host.allocated = True

        self.simulator.schedule("job.submitted", 1.0, job=self.job1,
                                hosts=self.hosts)

        assert self.job1.status == JobStatus.CREATED

        self.simulator.run()

        assert self.job1.status == JobStatus.QUEUED

    def test_multiple_queueing(self):
        self.simulator.schedule("job.submitted", 1.0, job=self.job1,
                                hosts=self.hosts)
        self.simulator.schedule("job.submitted", 2.0, job=self.job2,
                                hosts=self.hosts)
        self.simulator.schedule("job.submitted", 3.0, job=self.job3,
                                hosts=self.hosts)

        self.simulator.run_until(0.5)
        assert self.job1.status == JobStatus.CREATED

        self.simulator.run_until(1.5)
        assert self.job1.status == JobStatus.RUNNING

        self.simulator.run_until(2.5)
        assert self.job1.status == JobStatus.RUNNING
        assert self.job2.status == JobStatus.RUNNING

        self.simulator.run_until(3.5)
        assert self.job1.status == JobStatus.RUNNING
        assert self.job2.status == JobStatus.RUNNING
        assert self.job3.status == JobStatus.QUEUED
        assert self.scheduler.n_queued == 1

        self.simulator.run_until(6.5)
        assert self.job1.status == JobStatus.FINISHED
        assert self.job2.status == JobStatus.RUNNING
        assert self.job3.status == JobStatus.RUNNING

        self.simulator.run_until(7.5)
        assert self.job2.status == JobStatus.FINISHED
        assert self.job3.status == JobStatus.RUNNING

        self.simulator.run_until(11)
        assert self.job3.status == JobStatus.FINISHED

    def test_job_order(self):
        self.simulator.schedule("job.submitted", 1.0, job=self.job4,
                                hosts=self.hosts)
        self.simulator.schedule("job.submitted", 2.0, job=self.job5,
                                hosts=self.hosts)
        self.simulator.schedule("job.submitted", 3.0, job=self.job6,
                                hosts=self.hosts)

        self.simulator.run_until(1.5)
        assert self.job4.status == JobStatus.RUNNING
        assert self.job5.status == JobStatus.CREATED
        assert self.job6.status == JobStatus.CREATED

        self.simulator.run_until(2.5)
        assert self.job4.status == JobStatus.RUNNING
        assert self.job5.status == JobStatus.QUEUED
        assert self.job6.status == JobStatus.CREATED

        self.simulator.run_until(3.5)
        assert self.job4.status == JobStatus.RUNNING
        assert self.job5.status == JobStatus.QUEUED
        assert self.job6.status == JobStatus.QUEUED

        self.simulator.run_until(7)
        assert self.job4.status == JobStatus.FINISHED
        assert self.job5.status == JobStatus.RUNNING
        assert self.job6.status == JobStatus.QUEUED

        self.simulator.run_until(12)
        assert self.job4.status == JobStatus.FINISHED
        assert self.job5.status == JobStatus.FINISHED
        assert self.job6.status == JobStatus.RUNNING

        self.simulator.run_until(17)
        assert self.job4.status == JobStatus.FINISHED
        assert self.job5.status == JobStatus.FINISHED
        assert self.job6.status == JobStatus.FINISHED
