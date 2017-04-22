from itertools import groupby

from .host import Host
from .process import Process
from .process_mapper import LinearProcessMapper


class TestLinearProcessMapper:
    def setup(self):
        self.hosts = [Host("h" + str(i), capacity=8) for i in range(8)]
        self.procs = [Process("j1", rank=i) for i in range(60)]

    def test_map(self):
        mapper = LinearProcessMapper()
        mapping = mapper.map(self.procs, self.hosts)

        assert len(mapping.keys()) == 60

        for host, group in groupby(sorted(mapping.values())):
            assert len(list(group)) <= host.capacity
