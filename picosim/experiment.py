import yaml
import networkx as nx
from importlib import import_module

from .cluster import Cluster
from .job import Job
from .simulator import Simulator


class Experiment:
    def __init__(self, cluster, simulator):
        self.cluster = cluster
        self.simulator = simulator

    def run(self):
        self.simulator.run()

    @classmethod
    def from_yaml(cls, path):
        with open(path) as f:
            conf = yaml.load(f)

        simulator = Simulator()

        cluster = Cluster(
            graph=nx.read_graphml(conf["topology"]),
            host_selector=cls.load_class(conf["host_selector"]),
            process_mapper=cls.load_class(conf["process_mapper"]),
            scheduler=cls.load_class(conf["scheduler"]),
            router=cls.load_class(conf["router"]),
            simulator=simulator
        )

        for job_conf in conf["jobs"]:
            job = Job.from_trace(job_conf["trace"], job_conf["duration"],
                                 simulator=simulator)
            cluster.submit_job(job, time=job_conf["submit_at"])

        return Experiment(cluster, simulator)

    @classmethod
    def load_class(cls, path):
        mod_name, cls_name = path.rsplit(".", 1)
        mod = import_module(mod_name)

        return getattr(mod, cls_name)
