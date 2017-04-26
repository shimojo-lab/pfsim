from importlib import import_module
from logging import getLogger
from os import makedirs
from pathlib import Path

import networkx as nx

from schema import And, Or, Schema

import yaml

from .cluster import Cluster
from .collector import InterconnectMetricsCollector, SchedulerMetricsCollector
from .job import Job
from .simulator import Simulator

logger = getLogger(__name__)


class Experiment:
    SCENARIO_SCHEMA = Schema({
        "topology": And(str, lambda p: Path(p).exists(),
                        error="Topology file must exist"),
        "algorithms": {
            "scheduler": str,
            "host_selector": str,
            "process_mapper": str,
            "router": str
        },
        "output": str,
        "jobs": [{
            "submit_at": Or(float, int,
                            error="Job submission time must be a number"),
            "duration": Or(float, int,
                           error="Job duration must be a number"),
            "trace": And(str),
        }]
    })

    def __init__(self, cluster, simulator, output, collectors=[]):
        self.cluster = cluster
        self.simulator = simulator
        self.output = output
        self.collectors = collectors
        self.conf = None

    def run(self):
        self.report()

        self.simulator.run()

        makedirs(str(Path(self.output).parent), mode=0o777, exist_ok=True)
        nx.write_graphml(self.cluster.graph, self.output)

        for collector in self.collectors:
            collector.report()

    def report(self):
        logger.info("Starting experiment with following configuration:")
        logger.info("=" * 80)
        logger.info("Cluster Topology:          {0}".format(
            self.conf["topology"]))
        logger.info("Host Selection Algorithm:  {0}".format(
            self.conf["algorithms"]["host_selector"]))
        logger.info("Process Mapping Algorithm: {0}".format(
            self.conf["algorithms"]["process_mapper"]))
        logger.info("Routing Algorithm:         {0}".format(
            self.conf["algorithms"]["router"]))
        logger.info("Output Path:               {0}".format(
            self.conf["output"]))
        logger.info("=" * 80)

    @classmethod
    def from_yaml(cls, path):
        with open(path) as f:
            conf = cls._validate_conf(yaml.load(f))

        simulator = Simulator()

        algorithms_conf = conf["algorithms"]
        cluster = Cluster(
            graph=nx.read_graphml(conf["topology"]),
            host_selector=cls._load_class(algorithms_conf["host_selector"]),
            process_mapper=cls._load_class(algorithms_conf["process_mapper"]),
            scheduler=cls._load_class(algorithms_conf["scheduler"]),
            router=cls._load_class(algorithms_conf["router"]),
            simulator=simulator
        )

        for job_conf in conf["jobs"]:
            trace_path = Path(path).parent / job_conf["trace"]
            job = Job.from_trace(str(trace_path), job_conf["duration"],
                                 simulator=simulator)
            cluster.submit_job(job, time=job_conf["submit_at"])

        output = str(Path(path).parent / conf["output"])

        experiment = Experiment(cluster, simulator, output)

        experiment.collectors = [
            InterconnectMetricsCollector(simulator, cluster),
            SchedulerMetricsCollector(simulator, cluster)
        ]

        experiment.conf = conf

        return experiment

    @classmethod
    def _validate_conf(cls, conf):
        return cls.SCENARIO_SCHEMA.validate(conf)

    @classmethod
    def _load_class(cls, path):
        mod_name, cls_name = path.rsplit(".", 1)
        mod = import_module(mod_name)

        return getattr(mod, cls_name)
