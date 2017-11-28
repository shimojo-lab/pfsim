class Process:
    def __init__(self, job, rank):
        self.job = job
        self.rank = rank
        self.host = None
        self.job = None

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.job == other.job and self.rank == other.rank

    def __hash__(self):
        return hash((self.job, self.rank))

    def __repr__(self):  # pragma: no cover
        return "<Process {0} rank:{1} on {2}>".format(
            self.job, self.rank, self.host)
