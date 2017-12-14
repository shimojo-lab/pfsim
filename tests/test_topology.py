import networkx as nx

from pfsim.topology import NDTorusTopology


class TestTorusTopology:
    def test_1d(self):
        g = NDTorusTopology(1, [4]).generate()

        assert len(g) == 4
        assert all([d == 2 for d in g.in_degree().values()])
        assert all([d == 2 for d in g.out_degree().values()])
        assert nx.number_strongly_connected_components(g) == 1

    def test_2d(self):
        g = NDTorusTopology(2, [3, 4]).generate()

        assert len(g) == 12
        assert all([d == 4 for d in g.in_degree().values()])
        assert all([d == 4 for d in g.out_degree().values()])
        assert nx.number_strongly_connected_components(g) == 1

    def test_3d(self):
        g = NDTorusTopology(3, [3, 4, 5]).generate()

        assert len(g) == 60
        assert all([d == 6 for d in g.in_degree().values()])
        assert all([d == 6 for d in g.out_degree().values()])
        assert nx.number_strongly_connected_components(g) == 1

    def test_4d(self):
        g = NDTorusTopology(4, [4, 5, 6, 7]).generate()

        assert len(g) == 840
        assert all([d == 8 for d in g.in_degree().values()])
        assert all([d == 8 for d in g.out_degree().values()])
        assert nx.number_strongly_connected_components(g) == 1
