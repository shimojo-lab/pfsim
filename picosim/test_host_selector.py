from .host import Host
from .host_selector import LinearHostSelector, RandomHostSelector
from .job import Job


class TestLinearHostSelector:
    def setup(self):
        self.hosts = [Host("h" + str(i), capacity=8) for i in range(8)]

        self.job1 = Job("j1", n_procs=32)
        self.job2 = Job("j2", n_procs=16)
        self.job3 = Job("j3", n_procs=24)

        self.selector = LinearHostSelector()

    def test_reject_job(self):
        for i in range(7):
            self.hosts[i].allocated = True

        assert not self.selector.test(self.job1, self.hosts)

    def test_select_single_block(self):
        assert self.selector.select(self.job1, self.hosts) == self.hosts[:4]

    def test_select_multiple_blocks(self):
        for i in range(2, 6):
            self.hosts[i].allocated = True

        hosts = self.selector.select(self.job1, self.hosts)

        assert hosts == self.hosts[:2] + self.hosts[6:8]

    def test_select_smaller_block(self):
        self.hosts[3].allocated = True

        assert self.selector.select(self.job2, self.hosts) == self.hosts[:2]

    def test_minimize_blocks(self):
        self.hosts[1].allocated = True
        self.hosts[3].allocated = True
        self.hosts[5].allocated = True

        hosts = self.selector.select(self.job3, self.hosts)

        assert hosts == self.hosts[6:8] + self.hosts[:1]


class TestRandomHostSelector:
    def setup(self):
        self.hosts = [Host("h" + str(i), capacity=8) for i in range(8)]

        self.job1 = Job("j1", n_procs=8)
        self.job2 = Job("j2", n_procs=24)

        self.selector = RandomHostSelector()

    def test_single_host(self):
        hosts = self.selector.select(self.job1, self.hosts)

        assert len(hosts) == 1
        assert hosts[0] in self.hosts

    def test_multiple_hosts(self):
        hosts = self.selector.select(self.job2, self.hosts)

        assert len(hosts) == 3
        assert all([h in self.hosts for h in hosts])

    def test_sorted(self):
        hosts = self.selector.select(self.job2, self.hosts)

        assert sorted(hosts) == hosts
