import json
import tarfile


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
