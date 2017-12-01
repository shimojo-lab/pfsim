from copy import deepcopy
from itertools import product
from logging import getLogger
from logging.handlers import QueueHandler
from multiprocessing import Manager, Pool
from os import getpid
from pathlib import Path
from threading import Thread

from schema import Or, Schema

import yaml

from .scenario import Scenario

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
