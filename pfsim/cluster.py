from collections import defaultdict
from logging import getLogger

import networkx as nx

import yaml

from .host import Host
from .host_selector import HostSelector
from .process_mapper import ProcessMapper
from .router import Router
from .switch import Switch

logger = getLogger(__name__)


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
        assert issubclass(router, Router)

        self.scheduler = scheduler(hosts=self.hosts.values(),
                                   simulator=simulator,
                                   selector=host_selector(
                                       hosts=self.hosts.values()),
                                   mapper=process_mapper())
        self.router = router(graph=self.graph, hosts=self.hosts.values(),
                             switches=self.switches.values())
        self.simulator = simulator
        self.simulator.register("job.started", self._job_started)
        self.simulator.register("job.finished", self._job_finished)

        for u, v, attrs in self.graph.edges_iter(data=True):
            attrs["traffic"] = 0.0
            attrs["flows"] = 0

    def _update_fdbs(self, src, dst, path, job):
        for u, v in zip(path[1:-1], path[2:]):
            switch = self.switches[u]
            if v in self.switches:
                next_node = self.switches[v]
            elif v in self.hosts:
                next_node = self.hosts[v]

            switch.fdb.add(src, dst, next_node, job)

    def _job_started(self, job):
        host_tm = defaultdict(lambda: 0)
        host_fm = defaultdict(lambda: 0)

        for (src_rank, dst_rank), traffic in job.traffic_matrix.dok.items():
            src_host = job.procs[src_rank].host
            dst_host = job.procs[dst_rank].host

            host_tm[(src_host, dst_host)] += traffic
            host_fm[(src_host, dst_host)] += 1

        host_adj_list = list(host_tm.items())
        host_adj_list.sort(key=lambda x: x[1], reverse=True)

        for (src_host, dst_host), traffic in host_adj_list:
            path = self.router.route(src_host, dst_host, job)

            self._update_fdbs(src_host, dst_host, path, job)

            for u, v in zip(path[1:-1], path[2:-1]):
                edge = self.graph[u][v]
                edge["traffic"] += traffic
                edge["flows"] += host_fm[(src_host, dst_host)]
                job.link_usage[(u, v)] += traffic
                job.link_flows[(u, v)] += host_fm[(src_host, dst_host)]

        # Compute return paths if not already installed
        for (src_host, dst_host), traffic in host_adj_list:
            # Return path is already installed
            if self.router.cache.has(dst_host, src_host):
                continue

            # Compute return paths
            path = self.router.route(dst_host, src_host)
            self._update_fdbs(dst_host, src_host, path, job)

    def _job_finished(self, job):
        # Clear FDB for this job
        for switch in self.switches.values():
            switch.fdb.remove_job(job)

        # Clear routing cache for this job
        self.router.cache.remove_job(job)

        for u, v in job.link_usage.keys():
            self.graph[u][v]["traffic"] -= job.link_usage[(u, v)]
            self.graph[u][v]["flows"] -= job.link_flows[(u, v)]

    def submit_job(self, job, time=0.0):
        self.simulator.schedule("job.submitted", time, job=job)

    def report(self):
        logger.info("{0:=^80}".format(" " + self.graph.name + "  "))
        logger.info("Number of Hosts:           {0}".format(
            len(self.hosts)))
        logger.info("Number of Allocated Hosts: {0}".format(
            len([h for h in self.hosts.values() if h.allocated])))
        logger.info("Number of Switches:        {0}".format(
            len(self.switches)))
        logger.info("Number of Links:           {0}".format(
            self.graph.size()))
        logger.info("=" * 80)

    def write_flowtable(self, path):
        output = {}

        graph = nx.Graph(self.graph)
        mst = nx.minimum_spanning_tree(graph)
        # Links that need to be disabled
        diff = nx.difference(graph, mst)

        for switch in self.switches.values():
            output[switch.dpid] = {}
            output[switch.dpid]["fdb"] = {}

            # Write FDB
            for src in switch.fdb._fdb.keys():
                output[switch.dpid]["fdb"][src.mac] = {}

                for dst, nex in switch.fdb._fdb[src].items():
                    port = self.graph[switch.name][nex.name]["port"]
                    output[switch.dpid]["fdb"][src.mac][dst.mac] = port

            # Write list of blocked ports (to remove loops)
            output[switch.dpid]["blocked_ports"] = [
                self.graph[switch.name][u]["port"] for u in
                diff.neighbors_iter(switch.name)
            ]

        with open(path, "w") as f:
            yaml.dump(output, f, explicit_start=True, default_flow_style=False)
