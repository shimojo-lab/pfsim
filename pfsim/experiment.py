import logging.handlers
from copy import deepcopy
from importlib import import_module
from itertools import product
from logging import FileHandler, getLogger
from multiprocessing import Manager, Pool
from os import makedirs
from pathlib import Path
from threading import Thread

import networkx as nx

from schema import Or, Schema

import yaml

from .cluster import Cluster
from .collector import InterconnectMetricsCollector, SchedulerMetricsCollector
from .job_generator import JobGenerator
from .simulator import Simulator

logger = getLogger(__name__)

EXPERIMENT_CONF_SCHEMA = Schema({
    "duration": Or(int, float),
    "topology": str,
    "output": str,
    "algorithms": {
        "scheduler": [str],
        "host_selector": [str],
        "process_mapper": [str],
        "router": [str]
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
        log_path = Path(self.output_path) / "result.log"
        self.file_handler = FileHandler(str(log_path))

        self.conf = conf

    def run(self):
        getLogger().addHandler(self.file_handler)

        # Print simulator configurations
        self.report()
        # Print cluster configuration
        self.cluster.report()

        self.simulator.run_until(self.conf["duration"])

        for collector in self.collectors:
            collector.report()
            collector.write_csvs(self.output_path)

        getLogger().removeHandler(self.file_handler)

    def report(self):  # pragma: no cover
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

    def _load_class(self, path):
        mod_name, cls_name = path.rsplit(".", 1)
        mod = import_module(mod_name)

        return getattr(mod, cls_name)


def run_scenario(path, conf, q):
    q_handler = logging.handlers.QueueHandler(q)
    logger = getLogger()
    logger.handlers = []
    logger.addHandler(q_handler)

    scenario = Scenario(path, conf)
    scenario.run()

    logger.removeHandler(q_handler)


def logger_thread(q):
    while True:
        record = q.get()
        if record is None:
            break
        logger = getLogger(record.name)
        logger.handle(record)


class Experiment:
    def __init__(self, path):
        self.path = path
        with open(path) as f:
            self.conf = EXPERIMENT_CONF_SCHEMA.validate(yaml.load(f))

    def run_parallel(self, process):
        m = Manager()
        q = m.Queue()

        algorithm_conf = self.conf["algorithms"]
        schedulers = algorithm_conf["scheduler"]
        host_selectors = algorithm_conf["host_selector"]
        process_mappers = algorithm_conf["process_mapper"]
        routers = algorithm_conf["router"]

        algorithms = product(schedulers, host_selectors, process_mappers,
                             routers)

        lp = Thread(target=logger_thread, args=(q,))
        lp.start()

        with Pool(process) as pool:
            results = []
            for (sched, hs, pm, rt) in algorithms:
                scenario_conf = deepcopy(self.conf)
                algorithm_conf = scenario_conf["algorithms"]
                algorithm_conf["scheduler"] = sched
                algorithm_conf["host_selector"] = hs
                algorithm_conf["process_mapper"] = pm
                algorithm_conf["router"] = rt

                res = pool.apply_async(run_scenario, (self.path,
                                                      scenario_conf, q))
                results.append(res)
            pool.close()
            pool.join()

            for res in results:
                res.get()

        q.put(None)
        lp.join()

    def run_serial(self):
        base_path = Path(self.path).parent

        algorithm_conf = self.conf["algorithms"]
        schedulers = algorithm_conf["scheduler"]
        host_selectors = algorithm_conf["host_selector"]
        process_mappers = algorithm_conf["process_mapper"]
        routers = algorithm_conf["router"]

        algorithms = product(schedulers, host_selectors, process_mappers,
                             routers)

        for (sched, hs, pm, rt) in algorithms:
            scenario_conf = deepcopy(self.conf)
            algorithm_conf = scenario_conf["algorithms"]
            algorithm_conf["scheduler"] = sched
            algorithm_conf["host_selector"] = hs
            algorithm_conf["process_mapper"] = pm
            algorithm_conf["router"] = rt

            scenario = Scenario(base_path, scenario_conf)
            scenario.run()
