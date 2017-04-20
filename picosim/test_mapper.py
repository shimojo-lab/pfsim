from unittest.mock import patch

from .host import Host
from .job import Job

from .mapper import SimpleMapper


@patch("picosim.simulator.Simulator")
class TestSimpleMapper:
    def setup(self):
        self.hosts = [
            Host("h1", capacity=8),
            Host("h2", capacity=8),
            Host("h3", capacity=8),
            Host("h4", capacity=8),
            Host("h5", capacity=8),
            Host("h6", capacity=8),
            Host("h7", capacity=8),
            Host("h8", capacity=8),
        ]

        self.job1 = Job("j1", n_procs=60)

    def test_map(self, SimulatorMock):  # noqa: N803
        sim = SimulatorMock()
        mapper = SimpleMapper(sim)

        mapper.map(job=self.job1, allocated_hosts=self.hosts)

        assert sum([len(host.running_procs) for host in self.hosts]) == 60
        assert all([len(host.running_procs) > 0 for host in self.hosts])
