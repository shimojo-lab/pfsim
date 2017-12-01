from importlib import import_module
from logging import getLogger
from os import makedirs
from pathlib import Path

import networkx as nx

from prettytable import PrettyTable

from .cluster import Cluster
from .collector import InterconnectMetricsCollector, SchedulerMetricsCollector
from .job_generator import JobGenerator
from .simulator import Simulator

logger = getLogger(__name__)


class Scenario:
    def __init__(self, base_path, conf):
        # Create simulator
        self.simulator = Simulator()

        # Create cluster
        algorithms_conf = conf["algorithms"]
        self.cluster = Cluster(
            graph=nx.read_graphml(base_path / conf["topology"]),
            host_selector=self._load_class(algorithms_conf["host_selector"]),
            process_mapper=self._load_class(algorithms_conf["process_mapper"]),
            scheduler=self._load_class(algorithms_conf["scheduler"]),
            router=self._load_class(algorithms_conf["router"]),
            simulator=self.simulator
        )

        # Create job generators
        self.job_generators = []
        for job_conf in conf["jobs"]:
            job_submit_conf = job_conf["submit"]
            job_duration_conf = job_conf["duration"]

            submit_dist = self._load_class(job_submit_conf["distribution"])
            duration_dist = self._load_class(job_duration_conf["distribution"])

            job_gen = JobGenerator(
                submit=submit_dist(**job_submit_conf["params"]),
                duration=duration_dist(**job_duration_conf["params"]),
                trace=str(base_path / job_conf["trace"]),
                hosts=self.cluster.hosts,
                simulator=self.simulator
            )

            self.job_generators.append(job_gen)

        # Create metric collectors
        self.collectors = [
            InterconnectMetricsCollector(self.simulator, self.cluster),
            SchedulerMetricsCollector(self.simulator, self.cluster)
        ]

        # Create output directory and log handlers
        self.output_path = base_path / \
            conf["output"] / \
            algorithms_conf["scheduler"].split(".")[-1] / \
            algorithms_conf["host_selector"].split(".")[-1] / \
            algorithms_conf["process_mapper"].split(".")[-1] / \
            algorithms_conf["router"].split(".")[-1]
        makedirs(self.output_path, exist_ok=True)

        self.conf = conf

    def run(self):
        result_path = (Path(self.output_path) / "result.txt")

        with open(result_path, "w") as f:
            # Print simulator configurations
            self.report(f)
            # Print cluster configuration
            self.cluster.report(f)

            self.simulator.run_until(self.conf["duration"])

            for collector in self.collectors:
                collector.report(f)
                collector.write_csvs(self.output_path)

    def report(self, f):  # pragma: no cover
        table = PrettyTable()
        table.field_names = ["Item", "Value"]
        table.align = "l"

        table.add_row(["Duration", self.conf["duration"]])
        table.add_row(["Cluster Topology", self.conf["topology"]])
        table.add_row(["Host Section Algorithm",
                       self.conf["algorithms"]["host_selector"]])
        table.add_row(["Process Mapping ALgorith",
                       self.conf["algorithms"]["process_mapper"]])
        table.add_row(["Routing Algorithm",
                       self.conf["algorithms"]["router"]])

        f.write("Simulation Scenario\n")
        f.write(str(table))
        f.write("\n")

    def _load_class(self, path):
        mod_name, cls_name = path.rsplit(".", 1)
        mod = import_module(mod_name)

        return getattr(mod, cls_name)
