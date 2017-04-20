class Job:
    def __init__(self, name, n_procs=0, traffic_matrix=[]):
        self.name = name
        self.n_procs = n_procs
        self.traffic_matrix = traffic_matrix

    def __repr__(self):
        return "<Job {0} np:{1}>".format(self.name, self.n_procs)
