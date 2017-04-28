import json
import tarfile


class TrafficMatrix:
    _cache = {}

    def __init__(self, n_procs, coo=[]):
        self.n_procs = n_procs
        self.coo = coo

    def items(self):
        return self.coo

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
        coo = []
        n_procs = 0

        with tarfile.open(path, "r:gz") as tar:
            for member in tar.getmembers():
                if not member.isfile() or not member.name.endswith(".json"):
                    continue

                with tar.extractfile(member) as f:
                    trace = json.load(f)
                    src = trace["rank"]
                    n_procs += 1

                    for dst, tx_bytes in enumerate(trace["tx_bytes"]):
                        if tx_bytes <= 0.0:
                            continue

                        coo.append((src, dst, tx_bytes))

        # Inplace sort (src, dst, volume) tripltes by volume
        coo.sort(key=lambda triple: triple[2], reverse=True)

        matrix = cls(n_procs, coo)

        cls._cache[path] = matrix

        return matrix
