from unittest.mock import patch

from .host import Host
from .job import Job

from .scheduler import LinearScheduler


@patch("picosim.simulator.Simulator")
class TestLinearScheduler:
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

        self.job1 = Job("j1", n_procs=32)
        self.job2 = Job("j2", n_procs=16)
        self.job3 = Job("j3", n_procs=24)

    def test_reject_job(self, SimulatorMock):  # noqa: N803
        sim = SimulatorMock()
        scheduler = LinearScheduler(sim)

        for i in range(7):
            self.hosts[i].allocated = True

        scheduler.allocate(self.job1, self.hosts)

        sim.schedule.assert_called_once_with("job unaccepted")

    def test_allocate_single_block(self, SimulatorMock):  # noqa: N803
        sim = SimulatorMock()
        scheduler = LinearScheduler(sim)

        scheduler.allocate(self.job1, self.hosts)

        for i in range(4):
            assert self.hosts[i].allocated
            assert self.hosts[i].job == "j1"
        for i in range(5, 8):
            assert not self.hosts[i].allocated
            assert self.hosts[i].job is None

        sim.schedule.assert_called_once_with("job allocated")

    def test_allocate_multiple_blocks(self, SimulatorMock):  # noqa: N803
        sim = SimulatorMock()
        scheduler = LinearScheduler(sim)

        for i in range(2, 6):
            self.hosts[i].allocated = True

        scheduler.allocate(self.job1, self.hosts)

        for i in range(2):
            assert self.hosts[i].allocated
            assert self.hosts[i].job == "j1"
        for i in range(6, 8):
            assert self.hosts[i].allocated
            assert self.hosts[i].job == "j1"

        sim.schedule.assert_called_once_with("job allocated")

    def test_release_single_block(self, SimulatorMock):  # noqa: N803
        sim = SimulatorMock()
        scheduler = LinearScheduler(sim)

        for i in range(2, 6):
            self.hosts[i].allocated = True
            self.hosts[i].job = "j1"

        scheduler.release(self.job1, self.hosts)

        for i in range(2, 6):
            assert not self.hosts[i].allocated
            assert self.hosts[i].job is None

    def test_release_multiple_blocks(self, SimulatorMock):  # noqa: N803
        sim = SimulatorMock()
        scheduler = LinearScheduler(sim)

        for i in range(2):
            self.hosts[i].allocated = True
            self.hosts[i].job = "j1"
        for i in range(6, 8):
            self.hosts[i].allocated = True
            self.hosts[i].job = "j1"

        scheduler.release(self.job1, self.hosts)

        for i in range(2):
            assert not self.hosts[i].allocated
            assert self.hosts[i].job is None
        for i in range(6, 8):
            assert not self.hosts[i].allocated
            assert self.hosts[i].job is None

    def test_allocate_smaller_block(self, SimulatorMock):  # noqa: N803
        sim = SimulatorMock()
        scheduler = LinearScheduler(sim)

        self.hosts[3].allocated = True

        scheduler.allocate(self.job2, self.hosts)

        for i in range(2):
            assert self.hosts[i].allocated
            assert self.hosts[i].job == "j2"

    def test_minimize_blocks(self, SimulatorMock):  # noqa: N803
        sim = SimulatorMock()
        scheduler = LinearScheduler(sim)

        self.hosts[1].allocated = True
        self.hosts[3].allocated = True
        self.hosts[5].allocated = True

        scheduler.allocate(self.job3, self.hosts)

        assert self.hosts[0].allocated
        assert self.hosts[0].job == "j3"
        assert self.hosts[6].allocated
        assert self.hosts[6].job == "j3"
        assert self.hosts[7].allocated
        assert self.hosts[7].job == "j3"
