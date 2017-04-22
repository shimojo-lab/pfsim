import networkx as nx

from .host import Host
from .switch import Switch


class Cluster:
    def __init__(self, graphml):
        self.graph = nx.read_graphml(graphml)

        self.hosts = {}
        self.switches = {}

        for name, attrs in self.graph.nodes_iter(data=True):
            typ = attrs["typ"]

            if typ == "host":
                self.hosts[name] = Host(name, **attrs)
            elif typ == "switch":
                self.switches[name] = Switch(name, **attrs)

        for u, v, attrs in self.graph.edges_iter(data=True):
            attrs["traffic"] = 0
