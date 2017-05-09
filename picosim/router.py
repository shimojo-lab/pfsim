import random
from abc import ABC, abstractmethod
from collections import defaultdict
from heapq import heappop, heappush
from logging import getLogger
from math import inf

import networkx as nx

logger = getLogger(__name__)


class PathCache:
    def __init__(self):
        self._cache = {}
        self._jobs = defaultdict(list)

    def add(self, src, dst, path, job=None):
        self._cache[(src, dst)] = path

        if job is not None:
            self._jobs[job].append((src, dst))

    def has(self, src, dst):
        return (src, dst) in self._cache

    def get(self, src, dst):
        return self._cache.get((src, dst), None)

    def remove_job(self, job):
        if job not in self._jobs:
            return

        for src, dst in self._jobs[job]:
            if (src, dst) not in self._cache:
                continue

            del self._cache[(src, dst)]


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

        self.cache = PathCache()

    @abstractmethod
    def route(self, src, dst, job=None):
        pass


class RandomRouter(Router):
    def __init__(self, graph, hosts=None, switches=None):
        super().__init__(graph, hosts=hosts, switches=switches)

    def route(self, src, dst, job=None):
        if self.cache.has(src, dst):
            return self.cache.get(src, dst)

        paths = list(nx.all_shortest_paths(self.graph, src.name, dst.name))
        path = random.choice(paths)
        self.cache.add(src, dst, path)

        return path


class DmodKRouter(Router):
    def __init__(self, graph, hosts=None, switches=None):
        super().__init__(graph, hosts=hosts, switches=switches)

    def route(self, src, dst, job=None):
        if self.cache.has(src, dst):
            return self.cache.get(src, dst)

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

        path = paths[indices.pop()]
        self.cache.add(src, dst, path)

        return path


class GreedyRouter(Router):
    def __init__(self, graph, hosts=None, switches=None):
        super().__init__(graph, hosts=hosts, switches=switches)

    def route(self, src, dst, job=None):
        if self.cache.has(src, dst):
            return self.cache.get(src, dst)

        paths = list(nx.all_shortest_paths(self.graph, src.name, dst.name))
        min_path = paths[0]
        min_cost = inf

        for path in paths:
            cost = 0

            for u, v in zip(path[:-1], path[1:]):
                cost += self.graph[u][v]["traffic"]

            if cost < min_cost:
                min_path = path
                min_cost = cost

        self.cache.add(src, dst, min_path, job)

        return min_path


class GreedyRouter2(Router):
    def __init__(self, graph, hosts=None, switches=None):
        super().__init__(graph, hosts=hosts, switches=switches)

    def _widest_path(self, src, dst):
        q = []
        visited = set()

        heappush(q, (0, src, []))
        while q:
            cost, v, path = heappop(q)
            if v in visited:
                continue
            visited.add(v)

            if v == dst:
                return path + [dst]

            for u in self.graph.neighbors_iter(v):
                if u not in visited:
                    cost2 = self.graph[v][u]["traffic"]
                    heappush(q, (max(cost, cost2), u, path + [v]))

    def route(self, src, dst, job=None):
        if self.cache.has(src, dst):
            return self.cache.get(src, dst)

        paths = list(nx.all_shortest_paths(self.graph, src.name, dst.name))
        min_cost = inf
        min_path = paths[0]
        min_cost = inf

        for path in paths:
            cost = 0

            for u, v in zip(path[:-1], path[1:]):
                cost = max(self.graph[u][v]["traffic"], cost)

            if cost < min_cost:
                min_path = path
                min_cost = cost

        self.cache.add(src, dst, min_path, job)

        return min_path
