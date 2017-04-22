from .host import Host
from .host_selector import LinearHostSelector
from .job import Job


class TestLinearHostSelector:
    def setup(self):
        self.hosts = [Host("h" + str(i), capacity=8) for i in range(8)]

        self.job1 = Job("j1", n_procs=32)
        self.job2 = Job("j2", n_procs=16)
        self.job3 = Job("j3", n_procs=24)

    def test_reject_job(self):
        selector = LinearHostSelector()

        for i in range(7):
            self.hosts[i].allocated = True

        assert not selector.test(self.job1, self.hosts)

    def test_select_single_block(self):
        selector = LinearHostSelector()

        assert selector.select(self.job1, self.hosts) == self.hosts[:4]

    def test_select_multiple_blocks(self):
        selector = LinearHostSelector()

        for i in range(2, 6):
            self.hosts[i].allocated = True

        hosts = selector.select(self.job1, self.hosts)

        assert hosts == self.hosts[:2] + self.hosts[6:8]

    def test_select_smaller_block(self):
        selector = LinearHostSelector()

        self.hosts[3].allocated = True

        assert selector.select(self.job2, self.hosts) == self.hosts[:2]

    def test_minimize_blocks(self):
        selector = LinearHostSelector()

        self.hosts[1].allocated = True
        self.hosts[3].allocated = True
        self.hosts[5].allocated = True

        hosts = selector.select(self.job3, self.hosts)

        assert hosts == self.hosts[6:8] + self.hosts[:1]
