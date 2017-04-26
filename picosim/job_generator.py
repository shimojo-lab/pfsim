from .job import Job


class JobGenerator:
    def __init__(self, submit, duration, trace, hosts, simulator):
        self.submit_dist = submit
        self.duration_dist = duration
        self.trace = trace
        self.hosts = hosts
        self.simulator = simulator

        self.simulator.register("simulator.started", self._submit_job)
        self.simulator.register("job.submitted", self._submit_job)

    def _submit_job(self, job=None, **kwargs):
        if job and job.generator != self:
            return

        job = Job.from_trace(self.trace, self.duration_dist.get(),
                             generator=self, simulator=self.simulator)

        self.simulator.schedule_after("job.submitted", self.submit_dist.get(),
                                      job=job, hosts=self.hosts)
