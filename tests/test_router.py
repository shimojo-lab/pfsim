import networkx as nx

from pfsim.host import Host
from pfsim.router import DmodKRouter, GreedyRouter, GreedyRouter2, RandomRouter


class TestRandomRouter:
    def setup(self):
        self.graph = nx.DiGraph()
        self.graph.add_nodes_from(["h1", "h2", "s1", "s2", "s3", "s4"])
        self.graph.add_edges_from([("h1", "s1"), ("s1", "h1")])
        self.graph.add_edges_from([("s2", "h2"), ("h2", "s2")])
        self.graph.add_edges_from([("h1", "s2"), ("s2", "h1")])
        self.graph.add_edges_from([("s1", "h2"), ("h2", "s1")])
        self.graph.add_edges_from([("h1", "s3"), ("s3", "h1")])
        self.graph.add_edges_from([("s3", "s4"), ("s4", "s3")])
        self.graph.add_edges_from([("s4", "h2"), ("h2", "s4")])

        self.h1 = Host("h1", capacity=8)
        self.h2 = Host("h2", capacity=8)

        self.router = RandomRouter(self.graph)

    def test_intra_node(self):
        path = self.router.route(self.h1, self.h1)

        assert path == ["h1"]

    def test_inter_node(self):
        path = self.router.route(self.h1, self.h2)

        assert path == ["h1", "s1", "h2"] or path == ["h1", "s2", "h2"]

    def test_cache(self):
        path1 = self.router.route(self.h1, self.h2)
        path2 = self.router.route(self.h1, self.h2)

        assert path1 == path2


class TestDmodKRouter:
    def setup(self):
        self.graph = nx.DiGraph()
        self.graph.add_nodes_from(["h1", "h2", "h3", "h4"])
        self.graph.add_nodes_from(["s1", "s2", "s3", "s4", "s5", "s6"])
        self.graph.add_edges_from([("s1", "s3"), ("s3", "s1")])
        self.graph.add_edges_from([("s1", "s4"), ("s4", "s1")])
        self.graph.add_edges_from([("s2", "s3"), ("s3", "s2")])
        self.graph.add_edges_from([("s2", "s4"), ("s4", "s2")])
        self.graph.add_edges_from([("s3", "h1"), ("h1", "s3")])
        self.graph.add_edges_from([("s3", "h2"), ("h2", "s3")])
        self.graph.add_edges_from([("s4", "h3"), ("h3", "s4")])
        self.graph.add_edges_from([("s4", "h4"), ("h4", "s4")])

        self.h1 = Host("h1", capacity=8)
        self.h2 = Host("h2", capacity=8)
        self.h3 = Host("h3", capacity=8)
        self.h4 = Host("h4", capacity=8)

        self.hosts = [self.h1, self.h2, self.h3, self.h4]

        self.router = DmodKRouter(self.graph, hosts=self.hosts)

    def test_intra_node(self):
        path = self.router.route(self.h4, self.h4)

        assert path == ["h4"]

    def test_inter_node(self):
        path = self.router.route(self.h1, self.h4)
        assert path == ["h1", "s3", "s2", "s4", "h4"]

        path = self.router.route(self.h2, self.h4)
        assert path == ["h2", "s3", "s2", "s4", "h4"]

        path = self.router.route(self.h1, self.h3)
        assert path == ["h1", "s3", "s1", "s4", "h3"]

        path = self.router.route(self.h2, self.h3)
        assert path == ["h2", "s3", "s1", "s4", "h3"]

    def test_cache(self):
        path1 = self.router.route(self.h1, self.h2)
        path2 = self.router.route(self.h1, self.h2)

        assert path1 == path2


class TestGreedyRouter:
    def setup(self):
        self.graph = nx.DiGraph()
        self.graph.add_nodes_from(["h1", "h2"])
        self.graph.add_nodes_from(["s1", "s2", "s3", "s4"])

        self.graph.add_edge("h1", "s1", traffic=0)
        self.graph.add_edge("s1", "s2", traffic=1)
        self.graph.add_edge("s1", "s3", traffic=3)
        self.graph.add_edge("s2", "s4", traffic=4)
        self.graph.add_edge("s3", "s4", traffic=3)
        self.graph.add_edge("s4", "h2", traffic=0)

        self.graph.add_edge("s1", "h1", traffic=0)
        self.graph.add_edge("s2", "s1", traffic=0)
        self.graph.add_edge("s3", "s1", traffic=0)
        self.graph.add_edge("s4", "s2", traffic=0)
        self.graph.add_edge("s4", "s3", traffic=0)
        self.graph.add_edge("h2", "s4", traffic=0)

        self.h1 = Host("h1", capacity=8)
        self.h2 = Host("h2", capacity=8)

        self.hosts = [self.h1, self.h2]

        self.router = GreedyRouter2(self.graph, hosts=self.hosts)

    def test_inter_node(self):
        path = self.router.route(self.h1, self.h2)
        assert path == ["h1", "s1", "s3", "s4", "h2"]

    def test_cache(self):
        path1 = self.router.route(self.h1, self.h2)
        path2 = self.router.route(self.h1, self.h2)

        assert path1 == path2


class TestGreedyRouter2:
    def setup(self):
        self.graph = nx.DiGraph()
        self.graph.add_nodes_from(["h1", "h2"])
        self.graph.add_nodes_from(["s1", "s2", "s3", "s4"])

        self.graph.add_edge("h1", "s1", traffic=0)
        self.graph.add_edge("s1", "s2", traffic=1)
        self.graph.add_edge("s1", "s3", traffic=3)
        self.graph.add_edge("s2", "s4", traffic=4)
        self.graph.add_edge("s3", "s4", traffic=3)
        self.graph.add_edge("s4", "h2", traffic=0)

        self.graph.add_edge("s1", "h1", traffic=0)
        self.graph.add_edge("s2", "s1", traffic=0)
        self.graph.add_edge("s3", "s1", traffic=0)
        self.graph.add_edge("s4", "s2", traffic=0)
        self.graph.add_edge("s4", "s3", traffic=0)
        self.graph.add_edge("h2", "s4", traffic=0)

        self.h1 = Host("h1", capacity=8)
        self.h2 = Host("h2", capacity=8)

        self.hosts = [self.h1, self.h2]

        self.router = GreedyRouter(self.graph, hosts=self.hosts)

    def test_inter_node(self):
        path = self.router.route(self.h1, self.h2)
        assert path == ["h1", "s1", "s2", "s4", "h2"]

    def test_cache(self):
        path1 = self.router.route(self.h1, self.h2)
        path2 = self.router.route(self.h1, self.h2)

        assert path1 == path2
