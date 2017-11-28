import json
import tarfile

import networkx as nx


class TrafficMatrix:
    _cache = {}

    def __init__(self, n_procs, dok={}):
        self.n_procs = n_procs
        self.dok = dok

    def to_graph(self):
        g = nx.DiGraph()
        g.add_nodes_from(range(self.n_procs))

        for (src, dst), traffic in self.dok.items():
            g.add_edge(src, dst, traffic=traffic)

        return g

    def __len__(self):
        return len(self.dok)

    @property
    def density(self):
        return len(self) / self.n_procs ** 2

    @property
    def sparsity(self):
        return 1.0 - self.density

    @classmethod
    def _load_single_file(cls, f, dok):
        trace = json.load(f)
        src = trace["rank"]

        for dst, tx_messages in enumerate(trace["tx_messages"]):
            if tx_messages <= 0:
                continue

            dok[(src, dst)] = trace["tx_bytes"][dst]

    @classmethod
    def load(cls, f):
        if hasattr(f, "name") and f.name in cls._cache:
            return cls._cache[f.name]

        # c.f. https://www.wikiwand.com/en/Sparse_matrix
        dok = {}
        n_procs = 0

        with tarfile.open(fileobj=f, mode="r:*") as tar:
            for member in tar.getmembers():
                if not member.isfile() or not member.name.endswith(".json"):
                    continue

                with tar.extractfile(member) as f:
                    cls._load_single_file(f, dok)
                    n_procs += 1

        matrix = cls(n_procs, dok)

        if hasattr(f, "name"):
            cls._cache[f.name] = matrix

        return matrix
