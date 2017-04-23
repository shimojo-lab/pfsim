import networkx as nx

from .host import Host
from .process import Process
from .router import DmodKRouter, RandomRouter


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

        self.p0 = Process(None, 0)
        self.p1 = Process(None, 1)
        self.p2 = Process(None, 2)
        self.p3 = Process(None, 3)

        self.h1.procs = [self.p0, self.p1]
        self.h2.procs = [self.p2, self.p3]
        self.p0.host = self.h1
        self.p1.host = self.h1
        self.p2.host = self.h2
        self.p3.host = self.h2

    def test_intra_node(self):
        router = RandomRouter(self.graph)
        path = router.route(self.p0, self.p1)

        assert path == ["h1"]

    def test_inter_node(self):
        router = RandomRouter(self.graph)
        paths = router.route(self.p0, self.p2)

        assert paths == ["h1", "s1", "h2"] or paths == ["h1", "s2", "h2"]


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

        self.p0 = Process(None, 0)
        self.p1 = Process(None, 1)
        self.p2 = Process(None, 2)
        self.p3 = Process(None, 3)
        self.p4 = Process(None, 3)

        self.h1.procs = [self.p0]
        self.h2.procs = [self.p1]
        self.h3.procs = [self.p2]
        self.h4.procs = [self.p3, self.p4]
        self.p0.host = self.h1
        self.p1.host = self.h2
        self.p2.host = self.h3
        self.p3.host = self.h4
        self.p4.host = self.h4

        self.hosts = [self.h1, self.h2, self.h3, self.h4]

    def test_intra_node(self):
        router = DmodKRouter(self.graph, hosts=self.hosts)
        path = router.route(self.p3, self.p4)

        assert path == ["h4"]

    def test_inter_node(self):
        router = DmodKRouter(self.graph, hosts=self.hosts)

        path = router.route(self.p0, self.p3)
        assert path == ["h1", "s3", "s2", "s4", "h4"]

        path = router.route(self.p1, self.p3)
        assert path == ["h2", "s3", "s2", "s4", "h4"]

        path = router.route(self.p0, self.p2)
        assert path == ["h1", "s3", "s1", "s4", "h3"]

        path = router.route(self.p1, self.p2)
        assert path == ["h2", "s3", "s1", "s4", "h3"]
