from logging import getLogger

from .host import Host
from .host_selector import HostSelector
from .process_mapper import ProcessMapper
from .switch import Switch

logger = getLogger(__name__)


class Cluster:
    def __init__(self, graph, simulator=None, scheduler=None, router=None,
                 host_selector=None, process_mapper=None):

        self.graph = graph

        self.hosts = []
        self.switches = []

        for name, attrs in self.graph.nodes_iter(data=True):
            typ = attrs["typ"]

            if typ == "host":
                self.hosts.append(Host(name, **attrs))
            elif typ == "switch":
                self.switches.append(Switch(name, **attrs))

        assert issubclass(host_selector, HostSelector)
        assert issubclass(process_mapper, ProcessMapper)

        self.scheduler = scheduler(hosts=self.hosts,
                                   simulator=simulator,
                                   selector=host_selector(hosts=self.hosts),
                                   mapper=process_mapper())
        self.router = router(graph=self.graph, hosts=self.hosts,
                             switches=self.switches)
        self.simulator = simulator
        self.simulator.register("job.message", self._job_message)
        self.simulator.register("job.finished", self._job_finished)

        for u, v, attrs in self.graph.edges_iter(data=True):
            attrs["traffic"] = 0
            attrs["flows"] = 0

    def _job_message(self, job, src_proc, dst_proc, traffic):
        path = self.router.route(src_proc, dst_proc)

        for u, v in zip(path[1:], path[:-1]):
            edge = self.graph[u][v]
            edge["traffic"] += traffic
            edge["flows"] += 1
            job.link_usage[u][v] += traffic
            job.link_flows[u][v] += 1

    def _job_finished(self, job):
        for u, v_traffic in job.link_usage.items():
            for v, traffic in v_traffic.items():
                self.graph[u][v]["traffic"] -= traffic

        for u, v_flows in job.link_flows.items():
            for v, flows in v_flows.items():
                self.graph[u][v]["flows"] -= flows

    def submit_job(self, job, time=0.0):
        self.simulator.schedule("job.submitted", time, job=job)

    def report(self):
        logger.info("{0:=^80}".format(" " + self.graph.name + "  "))
        logger.info("Number of Hosts:           {0}".format(
            len(self.hosts)))
        logger.info("Number of Allocated Hosts: {0}".format(
            len([h for h in self.hosts if h.allocated])))
        logger.info("Number of Switches:        {0}".format(
            len(self.switches)))
        logger.info("Number of Links:           {0}".format(
            self.graph.size()))
        logger.info("=" * 80)
