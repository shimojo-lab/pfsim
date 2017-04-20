from .util import configure_logging

from .cluster import Cluster
from .mapper import SimpleMapper
from .router import RandomRouter
from .scheduler import LinearScheduler
from .simulator import Simulator


def main():
    configure_logging()

    scheduler = LinearScheduler()
    mapper = SimpleMapper()
    router = RandomRouter()

    cluster = Cluster("milk.graphml")

    simulator = Simulator()
    simulator.add([scheduler, mapper, router, cluster])
