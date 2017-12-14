from abc import ABC, abstractmethod

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
