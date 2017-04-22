import networkx as nx

from .cluster import Cluster
from .host_selector import LinearHostSelector
from .job import Job
from .process_mapper import LinearProcessMapper, RandomProcessMapper
from .router import RandomRouter
from .scheduler import FCFSScheduler
from .simulator import Simulator
from .util import configure_logging


def main():
    configure_logging()

    simulator = Simulator()
    scheduler = FCFSScheduler(simulator, LinearHostSelector(),
                              RandomProcessMapper())
    cluster = Cluster("milk.graphml")

    job = Job.from_trace("cg-c-128.tar.gz")
    simulator.schedule("job.submitted", job=job, hosts=cluster.hosts.values())

    router = RandomRouter()

    simulator.step()

    for src, vec in enumerate(job.traffic_matrix):
        for dst, traffic in enumerate(vec):
            src_proc = job.procs[src]
            dst_proc = job.procs[dst]

            path = router.route(src_proc, dst_proc, cluster.graph)
            for u, v in zip(path[1:], path[:-1]):
                edge = cluster.graph[u][v]
                edge["traffic"] += traffic

    congestion = max([attrs["traffic"] for u, v, attrs in cluster.graph.edges_iter(data=True)])
    print("Congestion:", congestion)

    nx.write_graphml(cluster.graph, "result.graphml")

    simulator.step()
