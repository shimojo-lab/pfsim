import json
import tarfile

from collections import defaultdict

import networkx as nx


class TrafficMatrix:
    _cache = {}

    def __init__(self, n_procs, dok={}):
        self.n_procs = n_procs
        self.dok = dok

    def adj_list(self):
        coo = []

        for (src, dst), traffic in self.dok.items():
            coo.append((src, dst, traffic))

        coo.sort(key=lambda x: x[2], reverse=True)

        return coo

    def reordered_adj_list(self, procs):
        host_traffic_matrix = defaultdict(lambda: 0)

        for src_rank, dst_rank, traffic in self.adj_list():
            src_host = procs[src_rank].host
            dst_host = procs[dst_rank].host

            host_traffic_matrix[(src_host, dst_host)] += traffic

        host_adj_list = list(host_traffic_matrix.items())
        host_adj_list.sort(key=lambda x: x[1], reverse=True)

        adj_list = []

        for (src_host, dst_host), traffic in host_adj_list:
            for src_proc in src_host.procs:
                for dst_proc in dst_host.procs:
                    src_rank = src_proc.rank
                    dst_rank = dst_proc.rank

                    if (src_rank, dst_rank) not in self.dok:
                        continue

                    traffic = self.dok[(src_rank, dst_rank)]

                    adj_list.append((src_rank, dst_rank, traffic))

        return adj_list

    def to_graph(self):
        g = nx.DiGraph()
        g.add_nodes_from(range(self.n_procs))

        for (src, dst), traffic in self.dok.items():
            g.add_edge(src, dst, traffic=traffic)

        return g

    def __len__(self):
        return len(self.coo)

    @property
    def density(self):
        return len(self) / self.n_procs ** 2

    @property
    def sparsity(self):
        return 1.0 - self.density

    @classmethod
    def load(cls, path):
        if path in cls._cache:
            return cls._cache[path]

        # c.f. https://www.wikiwand.com/en/Sparse_matrix
        dok = {}
        n_procs = 0

        with tarfile.open(path, "r:gz") as tar:
            for member in tar.getmembers():
                if not member.isfile() or not member.name.endswith(".json"):
                    continue

                with tar.extractfile(member) as f:
                    trace = json.load(f)
                    src = trace["rank"]
                    n_procs += 1

                    for dst, tx_messages in enumerate(trace["tx_messages"]):
                        if tx_messages <= 0:
                            continue

                        dok[(src, dst)] = trace["tx_bytes"][dst]

        matrix = cls(n_procs, dok)

        cls._cache[path] = matrix

        return matrix
