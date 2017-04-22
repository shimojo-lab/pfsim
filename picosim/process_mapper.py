import random
from abc import ABC, abstractmethod
from itertools import chain, repeat
from logging import getLogger

logger = getLogger(__name__)


class ProcessMapper(ABC):
    @abstractmethod
    def map(self, procs, hosts):
        pass


class LinearProcessMapper(ProcessMapper):
    def map(self, procs, hosts):
        repeated_hosts = [repeat(host, host.capacity) for host in hosts]
        return dict(zip(procs, chain(*repeated_hosts)))


class RandomProcessMapper(ProcessMapper):
    def map(self, procs, hosts):
        hosts = hosts.copy()
        random.shuffle(hosts)
        repeated_hosts = [repeat(host, host.capacity) for host in hosts]
        return dict(zip(procs, chain(*repeated_hosts)))
