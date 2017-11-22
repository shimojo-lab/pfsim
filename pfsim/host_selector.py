from abc import ABC, abstractmethod
from itertools import chain
from logging import getLogger
from random import choice

logger = getLogger(__name__)


class HostSelector(ABC):
    def __init__(self, hosts):
        self.hosts = hosts

    # Check if there are enough PEs to run the given job
    @abstractmethod
    def test(self, job):
        pass

    # Return a list of hosts which should be allocated to the given job
    @abstractmethod
    def select(self, job):
        pass

    def _available_pes(self, hosts):
        return sum([h.capacity for h in hosts if not h.allocated])


class LinearHostSelector(HostSelector):
    def test(self, job):
        return self._available_pes(self.hosts) >= job.n_procs

    def select(self, job):
        if not self.test(job):
            return None

        # Find all free blocks (consecutive unallocated hosts)
        unallocated_blocks = self._find_unallocated_blocks()

        # Are there any blocks larger than job.n_procs?
        # If yes, use the smallest one (to avoid fragmentation)
        cand_blocks = sorted([block for block in unallocated_blocks if
                              self._available_pes(block) >= job.n_procs],
                             key=len)
        if cand_blocks:
            return self._select_hosts_from_blocks(job, cand_blocks)

        # Allocate multiple blocks each smaller than job.n_procs
        cand_blocks = sorted(unallocated_blocks, key=len, reverse=True)

        return self._select_hosts_from_blocks(job, cand_blocks)

    def _find_unallocated_blocks(self):
        in_block = False
        unallocated_blocks = []

        for host in self.hosts:
            if in_block and host.allocated:
                in_block = False
            elif in_block and not host.allocated:
                unallocated_blocks[-1].append(host)
            elif not in_block and not host.allocated:
                unallocated_blocks.append([host])
                in_block = True

        return unallocated_blocks

    def _select_hosts_from_blocks(self, job, blocks):
        procs_togo = job.n_procs
        selected_hosts = []

        for host in chain(*blocks):
            if procs_togo <= 0:
                break

            procs_togo -= host.capacity
            selected_hosts.append(host)

        return selected_hosts


class RandomHostSelector(HostSelector):
    def test(self, job):
        return self._available_pes(self.hosts) >= job.n_procs

    def select(self, job):
        if not self.test(job):
            return None

        allocated_hosts = []
        procs_togo = job.n_procs

        while procs_togo > 0:
            host = choice([h for h in self.hosts if not h.allocated])
            allocated_hosts.append(host)
            procs_togo -= host.capacity

        return sorted(allocated_hosts)
