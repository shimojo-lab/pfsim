import random
from abc import ABC, abstractmethod
from logging import getLogger

import networkx as nx

logger = getLogger(__name__)


class Router(ABC):
    def __init__(self, graph, hosts=None, switches=None):
        self.graph = graph
        if hosts is None:
            hosts = []
        self.hosts = hosts
        if switches is None:
            switches = []
        self.switches = switches

        self.addrs = {host.name: i for i, host in enumerate(self.hosts)}

    @abstractmethod
    def route(self, src_proc, dst_proc):
        pass


class RandomRouter(Router):
    def __init__(self, graph, hosts=None, switches=None):
        super().__init__(graph, hosts=hosts, switches=switches)

    def route(self, src_proc, dst_proc):
        src = src_proc.host
        dst = dst_proc.host
        paths = list(nx.all_shortest_paths(self.graph, src.name, dst.name))

        return random.choice(paths)


class DmodKRouter(Router):
    def __init__(self, graph, hosts=None, switches=None):
        super().__init__(graph, hosts=hosts, switches=switches)

    def route(self, src_proc, dst_proc):
        src = src_proc.host
        dst = dst_proc.host

        paths = list(nx.all_shortest_paths(self.graph, src.name, dst.name))
        indices = list(range(len(paths)))

        for i in range(len(paths[0])):
            # Path decided
            if len(indices) == 1:
                break

            switches = sorted(list({paths[j][i] for j in indices}))

            # Only single option, skipping to next switch in path
            if len(switches) == 1:
                continue

            switch = switches[self.addrs[dst.name] % len(switches)]

            indices = [j for j in indices if paths[j][i] == switch]

        return paths[indices.pop()]
