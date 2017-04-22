import random
from abc import ABC, abstractmethod
from logging import getLogger

import networkx as nx

logger = getLogger(__name__)


class Router(ABC):
    @abstractmethod
    def route(self, src_proc, dst_proc):
        pass


class RandomRouter(Router):
    def route(self, src_proc, dst_proc, graph):
        src = src_proc.host
        dst = dst_proc.host
        paths = list(nx.all_shortest_paths(graph, src.name, dst.name,
                                           weight="weight"))

        return random.choice(paths)
