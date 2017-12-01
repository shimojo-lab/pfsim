from copy import deepcopy
from importlib import import_module
from itertools import product
from logging import getLogger
from logging.handlers import QueueHandler
from multiprocessing import Manager, Pool
from os import getpid, makedirs
from pathlib import Path
from threading import Thread

import networkx as nx

from prettytable import PrettyTable

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


def _run_scenario(path, conf):
    logger.info("Starting simulation at worker (PID %d)", getpid())

    scenario = Scenario(path, conf)
    scenario.run()

    logger.info("Finished simulation at worker (PID %d)", getpid())


def _logger_thread(queue):
    while True:
        record = queue.get()
        if record is None:
            break
        logger = getLogger(record.name)
        logger.handle(record)


def _set_q_handler(queue):
    q_handler = QueueHandler(queue)
    root_logger = getLogger()
    root_logger.handlers = []
    root_logger.addHandler(q_handler)

    logger.info("Starting worker proces at PID %d", getpid())


class Experiment:
    def __init__(self, path):
        self.path = path
        with open(path) as f:
            self.conf = EXPERIMENT_CONF_SCHEMA.validate(yaml.load(f))

    def run_parallel(self, degree_parallelism):
        base_path = Path(self.path).parent
        logger.info("Starting simulation in parallel mode "
                    "(%d worker processes)", degree_parallelism)
        logger.info("Using scenario file %s", Path(self.path).resolve())

        manager = Manager()
        log_q = manager.Queue()

        algorithm_conf = self.conf["algorithms"]
        schedulers = algorithm_conf["scheduler"]
        host_selectors = algorithm_conf["host_selector"]
        process_mappers = algorithm_conf["process_mapper"]
        routers = algorithm_conf["router"]

        algorithms = product(schedulers, host_selectors, process_mappers,
                             routers)

        thread = Thread(target=_logger_thread, args=(log_q,))
        thread.start()

        with Pool(degree_parallelism, _set_q_handler, (log_q,)) as pool:
            results = []
            for (sched, hs, pm, rt) in algorithms:
                scenario_conf = deepcopy(self.conf)
                algorithm_conf = scenario_conf["algorithms"]
                algorithm_conf["scheduler"] = sched
                algorithm_conf["host_selector"] = hs
                algorithm_conf["process_mapper"] = pm
                algorithm_conf["router"] = rt

                res = pool.apply_async(_run_scenario, (base_path,
                                                       scenario_conf))
                results.append(res)
            pool.close()
            pool.join()

            for res in results:
                res.get()

        log_q.put(None)
        thread.join()

    def run_serial(self):
        base_path = Path(self.path).parent
        logger.info("Starting simulation in serial mode")
        logger.info("Using scenario file %s", Path(self.path).resolve())

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

            _run_scenario(base_path, scenario_conf)
