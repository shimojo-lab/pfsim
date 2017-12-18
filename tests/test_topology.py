import networkx as nx

from pfsim.topology import NDTorusTopology, XGFTTopology


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


class TestXGFTTopology:
    #  Test cases are taken from X. Yuan, W. Nienaber, and S. Mahapatra, “ On
    #  Folded-Clos Networks with Deterministic Single-Path Routing,” ACM Trans.
    #  Parallel Comput., vol. 2, no. 4, pp. 1–22, Jan. 2016.

    def test_host(self):
        g = XGFTTopology(0, [], []).generate()

        assert len(g) == 1
        assert nx.number_strongly_connected_components(g) == 1
        assert nx.diameter(g) == 0

        hosts = [u for u, d in g.nodes(data=True) if d["typ"] == "host"]
        assert len(hosts) == 1

    def test_1lv_ft(self):
        g = XGFTTopology(1, [4], [1]).generate()

        assert len(g) == 5
        assert nx.number_strongly_connected_components(g) == 1
        assert nx.diameter(g) == 2

        print(g.nodes(data=True))
        hosts = [u for u, d in g.nodes(data=True) if d["typ"] == "host"]
        assert len(hosts) == 4
        switches = [u for u, d in g.nodes(data=True) if d["typ"] == "switch"]
        assert len(switches) == 1

    def test_2lv_ft(self):
        g = XGFTTopology(2, [4, 4], [1, 2]).generate()

        assert len(g) == 22
        assert nx.number_strongly_connected_components(g) == 1
        assert nx.diameter(g) == 4

        hosts = [u for u, d in g.nodes(data=True) if d["typ"] == "host"]
        assert len(hosts) == 16
        switches = [u for u, d in g.nodes(data=True) if d["typ"] == "switch"]
        assert len(switches) == 6

    def test_3lv_ft(self):
        g = XGFTTopology(3, [4, 4, 3], [1, 2, 2]).generate()

        assert len(g) == 70
        assert nx.number_strongly_connected_components(g) == 1
        assert nx.diameter(g) == 6

        hosts = [u for u, d in g.nodes(data=True) if d["typ"] == "host"]
        assert len(hosts) == 48
        switches = [u for u, d in g.nodes(data=True) if d["typ"] == "switch"]
        assert len(switches) == 22
