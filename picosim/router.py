from logging import getLogger

logger = getLogger(__name__)


class RandomRouter:
    def __init__(self, simulator):
        self.simulator = simulator
        simulator.register("routing requested", self.route)

    def route(self, src, dst, graph):
        pass
