class Switch:
    def __init__(self, name, **kwargs):
        # Switch name
        self.name = name

    def __repr__(self):
        return "<Switch {0}>".format(self.name)
