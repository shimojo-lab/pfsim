from abc import ABC, abstractmethod
from itertools import chain
from logging import getLogger

logger = getLogger(__name__)


class HostSelector(ABC):
    # Check if there are enough PEs to run the given job
    @abstractmethod
    def test(self, job, hosts):
        pass

    # Return a list of hosts which should be allocated to the given job
    @abstractmethod
    def select(self, job, hosts):
        pass


class LinearHostSelector(HostSelector):
    def _available_pes(self, block):
        return sum([h.capacity for h in block if not h.allocated])

    def test(self, job, hosts):
        return self._available_pes(hosts) >= job.n_procs

    def select(self, job, hosts):
        if not self.test(job, hosts):
            return None

        unallocated_blocks = []
        in_block = False

        # Find all free blocks (consecutive unallocated hosts)
        for host in hosts:
            if in_block and host.allocated:
                in_block = False
            elif in_block and not host.allocated:
                unallocated_blocks[-1].append(host)
            elif not in_block and not host.allocated:
                unallocated_blocks.append([host])
                in_block = True

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

    def _select_hosts_from_blocks(self, job, blocks):
        procs_togo = job.n_procs
        selected_hosts = []

        for host in chain(*blocks):
            if procs_togo <= 0:
                break

            procs_togo -= host.capacity
            selected_hosts.append(host)

        return selected_hosts