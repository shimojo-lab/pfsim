class Process:
    def __init__(self, job, rank, host=None):
        self.job = job
        self.rank = rank
        self.host = host

    def __repr__(self):
        return "<Process {0} rank:{1} on {2}>".format(
            self.job, self.rank, self.host)
