from .util import configure_logging

from .cluster import Cluster
from .process_mapper import LinearProcessMapper
from .router import RandomRouter
from .host_selector import LinearHostSelector
from .simulator import Simulator


def main():
    configure_logging()

    scheduler = LinearScheduler()
    mapper = SimpleMapper()
    router = RandomRouter()

    cluster = Cluster("milk.graphml")

    simulator = Simulator()
    simulator.add([scheduler, mapper, router, cluster])
