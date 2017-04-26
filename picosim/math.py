import random
from abc import ABC, abstractmethod


class Distribution(ABC):
    @abstractmethod
    def get():
        pass


class ExponentialDistribution(Distribution):
    def __init__(self, lambd):
        self.lambd = lambd

    def get(self):
        return random.expovariate(self.lambd)
