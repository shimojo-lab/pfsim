from abc import ABC, abstractmethod
from functools import reduce
from itertools import count, islice, product
from operator import mul

import networkx as nx


class Topology(ABC):
    @abstractmethod
    def generate(self):
        pass


class FileTopology(Topology):
    def __init__(self, base_path, path, **kwargs):
        self.path = str(base_path / path)

    def generate(self):
        return nx.read_graphml(self.path)


class NDTorusTopology(Topology):
    def __init__(self, n, dims, **kwargs):
        assert n == len(dims)
        self.n = n
        self.dims = dims
        self.bw = kwargs.get("bw", 1)
        self.npe = kwargs.get("npe", 1)

    def generate(self):
        idx = 0
        g = nx.DiGraph()

        def _generate(n, dims):
            nonlocal idx

            # 1-D torus
            if n == 1:
                nodes = range(idx, idx + dims[0])
                g.add_nodes_from(nodes, capacity=self.npe, typ="host")
                g.add_cycle(nodes, capacity=self.bw)
                g.add_cycle(reversed(nodes), capacity=self.bw)
                idx += dims[0]
                return

            tmp = idx

            # Generate dims[-1]x (n-1)-D torii
            for i in range(dims[-1]):
                _generate(n - 1, dims[:-1])

            # Connect dims[-1]x (n-1)-D torii
            sz = reduce(mul, dims[:-1], 1)
            for i in range(tmp, tmp + sz):
                indices = range(i, dims[-1] * sz + i, sz)
                g.add_cycle(indices, capacity=self.bw)
                g.add_cycle(reversed(indices), capacity=self.bw)

        _generate(self.n, self.dims)

        return g


class XGFTTopology(Topology):
    def __init__(self, h, m, w, **kwargs):
        assert h == len(m)
        assert h == len(w)
        self.h = h
        self.m = m
        self.w = w
        self.bw = kwargs.get("bw", 1)
        self.npe = kwargs.get("npe", 1)

    def generate(self):
        idx = count()
        g = nx.DiGraph()

        def _generate(h, m, w):
            if h == 0:
                i = next(idx)
                g.add_node(i, typ="host", capacity=self.npe)
                return [i]

            spines = list(islice(idx, reduce(mul, w, 1)))
            g.add_nodes_from(spines, typ="switch")

            for i in range(m[-1]):
                leaves = _generate(h - 1, m[:-1], w[:-1])
                g.add_edges_from(product(spines, leaves), capacity=self.bw)
                g.add_edges_from(product(leaves, spines), capacity=self.bw)

            return spines

        _generate(self.h, self.m, self.w)

        return g
