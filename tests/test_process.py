from pfsim.job import Job
from pfsim.process import Process


class TestProcess:
    def test_eq(self):
        j1 = Job("job1")
        j1 = Job("job2")
        p1 = Process(j1, 0)
        p2 = Process(j1, 1)
        p3 = Process(j1, 0)

        assert p1 == p1
        assert p1 != p2
        assert p1 == p3
        assert p1 != j1
