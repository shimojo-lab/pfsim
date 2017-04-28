from collections import defaultdict


class FDB:
    def __init__(self):
        self._fdb = defaultdict(dict)
        self._jobs = defaultdict(list)

    def add(self, src, dst, next_node, job):
        self._fdb[src][dst] = next_node
        self._jobs[job].append((src, dst))

    def remove_job(self, job):
        for src, dst in self._jobs[job]:
            if dst not in self._fdb[src]:
                continue
            del self._fdb[src][dst]

            if not self._fdb[src]:
                del self._fdb[src]

    def dump(self):
        for src in self._fdb.keys():
            for dst, next_node in self._fdb[src].items():
                print("\t{0} -> {1}: {2}".format(src.name, dst.name,
                      next_node.name))


class Switch:
    def __init__(self, name, **kwargs):
        # Switch name
        self.name = name
        # Forwarding DataBase
        self.fdb = FDB()

    def __repr__(self):
        return "<Switch {0}>".format(self.name)
