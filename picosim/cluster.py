from .host import Host
from .switch import Switch


class Cluster:
    def __init__(self, graph, simulator=None, scheduler=None, router=None,
                 host_selector=None, process_mapper=None):

        self.graph = graph

        self.hosts = {}
        self.switches = {}

        for name, attrs in self.graph.nodes_iter(data=True):
            typ = attrs["typ"]

            if typ == "host":
                self.hosts[name] = Host(name, **attrs)
            elif typ == "switch":
                self.switches[name] = Switch(name, **attrs)

        self.scheduler = scheduler(simulator=simulator,
                                   selector=host_selector(),
                                   mapper=process_mapper())
        self.router = router()
        self.simulator = simulator
        self.simulator.register("job.communicate", self.communicate)

        for u, v, attrs in self.graph.edges_iter(data=True):
            attrs["traffic"] = 0

    def communicate(self, src_proc, dst_proc, traffic):
        path = self.router.route(src_proc, dst_proc, self.graph)
        for u, v in zip(path[1:], path[:-1]):
            edge = self.graph[u][v]
            edge["traffic"] += traffic

    def submit_job(self, job, time=0.0):
        self.simulator.schedule("job.submitted", job=job, time=time,
                                hosts=self.hosts.values())
