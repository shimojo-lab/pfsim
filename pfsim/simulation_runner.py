from logging import getLogger
from logging.handlers import QueueHandler
from multiprocessing import Manager, Pool
from os import getpid
from pathlib import Path
from threading import Thread

import yaml

from .configuration import Scenario
from .simulation import Simulation

logger = getLogger(__name__)


def _run_scenario(path, conf):
    logger.info("Starting simulation at worker (PID %d)", getpid())

    scenario = Simulation(path, conf)
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


class SimulationRunner:
    def __init__(self, path):
        self.path = path
        with open(path) as f:
            conf = yaml.load(f)
            self.confs = Scenario.generate_from_yaml(conf)

    def run_parallel(self, degree_parallelism):
        base_path = Path(self.path).parent
        logger.info("Starting simulation in parallel mode "
                    "(%d worker processes)", degree_parallelism)
        logger.info("Using scenario file %s", Path(self.path).resolve())

        manager = Manager()
        log_q = manager.Queue()

        thread = Thread(target=_logger_thread, args=(log_q,))
        thread.start()

        with Pool(degree_parallelism, _set_q_handler, (log_q,)) as pool:
            results = []
            for conf in self.confs:
                res = pool.apply_async(_run_scenario, (base_path, conf))
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

        for conf in self.confs:
            _run_scenario(base_path, conf)
