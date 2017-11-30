import json
import tarfile
from io import BytesIO
from math import isclose

from pfsim.traffic_matrix import TrafficMatrix


FIXTURES = [
    {
        "rank": 0,
        "tx_messages": [0, 1, 1, 0],
        "tx_bytes": [0, 100, 200, 0]
    },
    {
        "rank": 1,
        "tx_messages": [1, 0, 0, 0],
        "tx_bytes": [150, 0, 0, 0]
    },
    {
        "rank": 2,
        "tx_messages": [0, 1, 0, 1],
        "tx_bytes": [0, 250, 0, 150]
    },
    {
        "rank": 3,
        "tx_messages": [0, 0, 1, 0],
        "tx_bytes": [0, 0, 100, 0]
    },
]


class TestTrafficMatrix:
    def setup(self):
        self.file = BytesIO()

        with tarfile.open(mode="w:gz", fileobj=self.file) as tar:
            for i, fixture in enumerate(FIXTURES):
                buf = json.dumps(fixture).encode()

                with BytesIO(buf) as f:
                    fname = "pfprof{0}.json".format(i)
                    tarinfo = tarfile.TarInfo(name=fname)
                    tarinfo.size = len(buf)
                    tar.addfile(tarinfo, fileobj=f)

        self.file.seek(0)

    def teardown(self):
        self.file.close()

    def test_load(self):
        tm = TrafficMatrix.load(self.file)
        assert tm.n_procs == 4
        assert tm.dok == {
            (0, 1): 100,
            (0, 2): 200,
            (1, 0): 150,
            (2, 1): 250,
            (2, 3): 150,
            (3, 2): 100
        }

    def test_len(self):
        tm = TrafficMatrix.load(self.file)
        assert len(tm) == 6

    def test_sparsity(self):
        tm = TrafficMatrix.load(self.file)
        assert isclose(tm.sparsity, 1 - 6 / 16)

    def test_density(self):
        tm = TrafficMatrix.load(self.file)
        assert isclose(tm.density, 6 / 16)

    def test_graph(self):
        tm = TrafficMatrix.load(self.file)
        g = tm.to_graph()

        assert len(g) == 4
        assert g.has_edge(0, 1)
        assert g.has_edge(0, 2)
        assert g.has_edge(1, 0)
        assert g.has_edge(2, 1)
        assert g.has_edge(2, 3)
        assert g.has_edge(3, 2)
