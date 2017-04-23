class Host:
    def __init__(self, name, capacity=1, **kwargs):
        # Host name
        self.name = name
        # Maximum number of processes which this host can accomodate
        self.capacity = capacity
        # Whether this host is allocated to a job or not
        self.allocated = False
        # Name of the job which this host is allocated to
        self.job = None
        # List of processes running on this host
        self.procs = []

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.name == other.name

    def __lt__(self, other):
        return self.name < other.name

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return "<Host {0} cap:{1} alloc:{2} job:{3}>".format(
            self.name, self.capacity, self.allocated, self.job)
