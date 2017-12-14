from importlib import import_module
from logging import getLogger
from os import makedirs
from pathlib import Path

from prettytable import PrettyTable

from .cluster import Cluster
from .collector import InterconnectMetricsCollector, SchedulerMetricsCollector
from .job_generator import JobGenerator
from .simulator import Simulator

logger = getLogger(__name__)


class Simulation:
    def __init__(self, base_path, conf):
        # Create simulator
        self.simulator = Simulator()

        # Create topology
        params = conf.topology.params.copy()
        params["base_path"] = base_path
        topo = self._load_class(conf.topology.kind)
        graph = topo(**params).generate()

        # Create cluster
        self.cluster = Cluster(
            graph=graph,
            host_selector=self._load_class(conf.host_selector),
            process_mapper=self._load_class(conf.process_mapper),
            scheduler=self._load_class(conf.scheduler),
            router=self._load_class(conf.router),
            simulator=self.simulator
        )

        # Create job generators
        self.job_generators = []
        for job_conf in conf.jobs:
            submit_dist = self._load_class(job_conf.submit_dist)
            duration_dist = self._load_class(job_conf.duration_dist)

            job_gen = JobGenerator(
                submit=submit_dist(**job_conf.submit_params),
                duration=duration_dist(**job_conf.duration_params),
                trace=str(base_path / job_conf.trace),
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
        self.output_path = base_path / conf.output / str(conf.id)

        makedirs(str(self.output_path), exist_ok=True)

        self.conf = conf

    def run(self):
        result_path = (Path(self.output_path) / "result.txt")

        with open(str(result_path), "w") as f:
            # Print simulator configurations
            self.report(f)
            # Print cluster configuration
            self.cluster.report(f)

            self.simulator.run_until(self.conf.duration)

            for collector in self.collectors:
                collector.report(f)
                collector.write_csvs(self.output_path)

    def report(self, f):  # pragma: no cover
        table = PrettyTable()
        table.field_names = ["Item", "Value"]
        table.align = "l"

        table.add_row(["Duration", self.conf.duration])
        table.add_row(["Cluster Topology", self.conf.topology])
        table.add_row(["Host Section Algorithm", self.conf.host_selector])
        table.add_row(["Process Mapping Algorithm", self.conf.process_mapper])
        table.add_row(["Routing Algorithm", self.conf.router])

        f.write("Simulation Scenario\n")
        f.write(str(table))
        f.write("\n")

    def _load_class(self, path):
        mod_name, cls_name = path.rsplit(".", 1)

        try:
            mod = import_module(mod_name)
        except ImportError:
            logger.error("Could not load module %s", mod_name)
            raise

        if not hasattr(mod, cls_name):
            logger.error("Module %s does not contain class %s",
                         mod_name, cls_name)
            raise

        return getattr(mod, cls_name)
