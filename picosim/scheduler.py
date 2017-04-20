from logging import getLogger

logger = getLogger(__name__)


class LinearScheduler:
    def __init__(self, simulator):
        simulator.register("job submitted", self.allocate)
        simulator.register("job finished", self.release)
        self.simulator = simulator

    def allocate(self, job, hosts):
        def available_pes(block):
            return sum([h.capacity for h in block if not h.allocated])

        # Check if job can be accomodated
        if available_pes(hosts) < job.n_procs:
            logger.warning("Refusing job {0}".format(job.name))
            self.simulator.schedule("job unaccepted")
            return

        unallocated_blocks = []
        in_block = False

        # Search all free blocks (consecutive unallocated hosts)
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
                              available_pes(block) >= job.n_procs], key=len)
        if cand_blocks:
            self._allocate_block(job, job.n_procs, cand_blocks[0])
            self.simulator.schedule("job allocated")
            return

        # Allocate multiple blocks each smaller than job.n_procs
        cand_blocks = sorted(unallocated_blocks, key=len, reverse=True)
        procs_togo = job.n_procs
        for block in cand_blocks:
            if procs_togo <= 0:
                break

            procs_togo -= self._allocate_block(job, procs_togo, block)

        self.simulator.schedule("job allocated")

    def _allocate_block(self, job, procs_togo, block):
        allocated_pes = 0

        for host in block:
            if procs_togo <= 0:
                break

            host.allocated = True
            host.job = job.name

            procs_togo -= host.capacity
            allocated_pes += host.capacity

        return allocated_pes

    def release(self, job, hosts):
        for host in hosts:
            if job.name == host.job:
                host.allocated = False
                host.job = None
