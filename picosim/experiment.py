from importlib import import_module
from logging import getLogger
from pathlib import Path

import networkx as nx

from schema import And, Or, Schema

import yaml

from .cluster import Cluster
from .collector import InterconnectMetricsCollector, SchedulerMetricsCollector
from .job_generator import JobGenerator
from .simulator import Simulator

logger = getLogger(__name__)


class Experiment:
    SCENARIO_SCHEMA = Schema({
        "duration": Or(int, float),
        "topology": And(str, lambda p: Path(p).exists(),
                        error="Topology file must exist"),
        "algorithms": {
            "scheduler": str,
            "host_selector": str,
            "process_mapper": str,
            "router": str
        },
        "jobs": [{
            "submit": {
                "distribution": str,
                "params": {
                    str: Or(str, int, float)
                }
            },
            "duration": {
                "distribution": str,
                "params": {
                    str: Or(str, int, float)
                }
            },
            "trace": str
        }]
    })

    def __init__(self, cluster, simulator, collectors=[]):
        self.cluster = cluster
        self.simulator = simulator
        self.collectors = collectors
        self.conf = None
        self.job_generators = []

    def run(self):
        self.report()
        self.cluster.report()

        self.simulator.run_until(self.conf["duration"])

        for collector in self.collectors:
            collector.report()

    def report(self):
        logger.info("Starting experiment with following configuration:")
        logger.info("=" * 80)
        logger.info("Duration:                  {0}".format(
            self.conf["duration"]))
        logger.info("Cluster Topology:          {0}".format(
            self.conf["topology"]))
        logger.info("Host Selection Algorithm:  {0}".format(
            self.conf["algorithms"]["host_selector"]))
        logger.info("Process Mapping Algorithm: {0}".format(
            self.conf["algorithms"]["process_mapper"]))
        logger.info("Routing Algorithm:         {0}".format(
            self.conf["algorithms"]["router"]))
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

        experiment = Experiment(cluster, simulator)

        for job_conf in conf["jobs"]:
            job_submit_conf = job_conf["submit"]
            job_duration_conf = job_conf["duration"]

            submit_dist = cls._load_class(job_submit_conf["distribution"])
            duration_dist = cls._load_class(job_duration_conf["distribution"])

            job_gen = JobGenerator(
                submit=submit_dist(**job_submit_conf["params"]),
                duration=duration_dist(**job_duration_conf["params"]),
                trace=str(Path(path).parent / job_conf["trace"]),
                hosts=cluster.hosts,
                simulator=simulator
            )

            experiment.job_generators.append(job_gen)

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
