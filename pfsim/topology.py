from abc import ABC, abstractmethod
from functools import reduce
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
    def __init__(self, n, dims):
        assert n == len(dims)
        self.n = n
        self.dims = dims

    def generate(self):
        idx = 0
        g = nx.DiGraph()

        def _generate(n, dims):
            nonlocal idx
            nonlocal g

            # 1-D torus
            if n == 1:
                indices = range(idx, idx + dims[0])
                g.add_cycle(indices)
                g.add_cycle(reversed(indices))
                idx += dims[0]
                return g

            tmp = idx

            # Generate dims[-1]x (n-1)-D torii
            for i in range(dims[-1]):
                _generate(n - 1, dims[:-1])

            # Connect dims[-1]x (n-1)-D torii
            sz = reduce(mul, dims[:-1], 1)
            for i in range(tmp, tmp + sz):
                indices = range(i, dims[-1] * sz + i, sz)
                print(list(indices))
                g.add_cycle(indices)
                g.add_cycle(reversed(indices))

        _generate(self.n, self.dims)

        return g
