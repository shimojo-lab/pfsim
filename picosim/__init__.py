import logging.config

from .host import Host
from .job import Job
from .scheduler import LinearScheduler
from .simulator import Simulator


def configure_logging():
    logging.config.dictConfig({
        "version": 1,
        "formatters": {
            "color": {
                "()": "colorlog.ColoredFormatter",
                "format": "%(log_color)s[%(levelname)s]%(reset)s [%(name)s]: "
                          "%(message)s",
                "datefmt": "%H:%M:%S"
            }
        },
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "formatter": "color",
                "stream": "ext://sys.stdout",
            }
        },
        "root": {
            "level": "INFO",
            "handlers": ["default"],
        },
        "disable_existing_loggers": False
    })


def main():
    configure_logging()

    sim = Simulator()

    hosts = [
        Host("h1", capacity=8),
        Host("h2", capacity=8),
        Host("h3", capacity=8),
        Host("h4", capacity=8),
        Host("h5", capacity=8),
        Host("h6", capacity=8),
        Host("h7", capacity=8),
        Host("h8", capacity=8),
    ]

    job1 = Job("testjob1", n_procs=32)
    job2 = Job("testjob2", n_procs=16)
    job3 = Job("testjob3", n_procs=16)
    job4 = Job("testjob4", n_procs=48)

    sim.schedule("job submitted", 1.0, job=job1, hosts=hosts)
    sim.schedule("job submitted", 2.0, job=job2, hosts=hosts)
    sim.schedule("job submitted", 3.0, job=job3, hosts=hosts)
    sim.schedule("job submitted", 4.0, job=job4, hosts=hosts)

    sim.schedule("job finished", 5.0, job=job1, hosts=hosts)
    sim.schedule("job finished", 6.0, job=job3, hosts=hosts)
    sim.schedule("job submitted", 7.0, job=job4, hosts=hosts)

    scheduler = LinearScheduler(sim)

    sim.run()

    for host in hosts:
        print(host)
