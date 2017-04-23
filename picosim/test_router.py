import networkx as nx

from .host import Host
from .process import Process
from .router import RandomRouter


class TestRandomRouter:
    def setup(self):
        self.graph = nx.DiGraph()
        self.graph.add_nodes_from(["h1", "h2", "s1", "s2", "s3", "s4"])
        self.graph.add_edge("h1", "s1")
        self.graph.add_edge("s2", "h2")
        self.graph.add_edge("h1", "s2")
        self.graph.add_edge("s1", "h2")
        self.graph.add_edge("h1", "s3")
        self.graph.add_edge("s3", "s4")
        self.graph.add_edge("s4", "h2")

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
        router = RandomRouter()
        path = router.route(self.p0, self.p1, self.graph)

        assert path == ["h1"]

    def test_inter_node(self):
        router = RandomRouter()
        paths = router.route(self.p0, self.p2, self.graph)

        assert paths == ["h1", "s1", "h2"] or paths == ["h1", "s2", "h2"]
