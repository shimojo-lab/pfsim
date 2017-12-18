import random
from abc import ABC, abstractmethod
from itertools import chain, repeat, zip_longest
from logging import getLogger

logger = getLogger(__name__)


class ProcessMapper(ABC):
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def map(self, procs, hosts):  # pragma: no cover
        pass


class LinearProcessMapper(ProcessMapper):
    def map(self, procs, hosts):
        repeated_hosts = [repeat(host, host.capacity) for host in hosts]
        return dict(zip(procs, chain(*repeated_hosts)))


class CyclicProcessMapper(ProcessMapper):
    def map(self, procs, hosts):
        repeated_hosts = [repeat(host, host.capacity) for host in hosts]
        return dict(zip(procs, chain(*[[h for h in tpl if h] for tpl in
                                       zip_longest(*repeated_hosts)])))


class RandomProcessMapper(ProcessMapper):
    def map(self, procs, hosts):
        hosts = hosts.copy()
        random.shuffle(hosts)
        repeated_hosts = [repeat(host, host.capacity) for host in hosts]
        return dict(zip(procs, chain(*repeated_hosts)))
