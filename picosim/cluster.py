from .host import Host
from .switch import Switch

from .host_selector import HostSelector
from .process_mapper import ProcessMapper


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

        assert issubclass(host_selector, HostSelector)
        assert issubclass(process_mapper, ProcessMapper)

        self.scheduler = scheduler(simulator=simulator,
                                   selector=host_selector(),
                                   mapper=process_mapper())
        self.router = router(graph=self.graph, hosts=self.hosts.values(),
                             switches=self.switches.values())
        self.simulator = simulator
        self.simulator.register("job.message", self._job_message)
        self.simulator.register("job.finished", self._job_finished)

        for u, v, attrs in self.graph.edges_iter(data=True):
            attrs["traffic"] = 0

    def _job_message(self, job, src_proc, dst_proc, traffic):
        path = self.router.route(src_proc, dst_proc)

        for u, v in zip(path[1:], path[:-1]):
            edge = self.graph[u][v]
            edge["traffic"] += traffic
            job.link_usage[u][v] += traffic

    def _job_finished(self, job, hosts):
        for u, v_traffic in job.link_usage.items():
            for v, traffic in v_traffic.items():
                self.graph[u][v]["traffic"] -= traffic

    def submit_job(self, job, time=0.0):
        self.simulator.schedule("job.submitted", job=job, time=time,
                                hosts=self.hosts.values())
