<<<<<<< HEAD
from pfsim.job import Job
from pfsim.simulator import Simulator
=======
from pfsim.simulator import Simulator
from pfsim.job import Job
>>>>>>> Output the log one by one


class TestJob:
    def setup(self):
        self.sim = Simulator()

    def test_timing(self):
        job1 = Job("job1", simulator=self.sim)
        job2 = Job("job2", simulator=self.sim)

        self.sim.schedule("job.submitted", 1.0, job=job1)
        self.sim.schedule("job.started", 2.0, job=job1)
        self.sim.schedule("job.finished", 3.0, job=job1)

        self.sim.schedule("job.submitted", 2.5, job=job2)
        self.sim.schedule("job.started", 3.5, job=job2)
        self.sim.schedule("job.finished", 4.5, job=job2)

        self.sim.run()

        assert job1.created_at == 1.0
        assert job1.started_at == 2.0
        assert job1.finished_at == 3.0

        assert job2.created_at == 2.5
        assert job2.started_at == 3.5
        assert job2.finished_at == 4.5
