class Host:
    def __init__(self, name, capacity=0, **kwargs):
        # Host name
        self.name = name
        # Maximum number of processes which this host can accomodate
        self.capacity = capacity
        # Whether this host is allocated to a job or not
        self.allocated = False
        # Name of the job which this host is allocated to
        self.job = None
        # List of processes running on this host
        self.running_procs = []

    def __repr__(self):
        return "<Host {0} cap:{1} alloc:{2} job:{3}>".format(
            self.name, self.capacity, self.allocated, self.job)
