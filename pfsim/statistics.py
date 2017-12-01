import csv
from bisect import bisect_right
from logging import getLogger
from math import inf, nan, sqrt

from prettytable import PrettyTable


logger = getLogger(__name__)


class Samples:
    def __init__(self, name, store=True):
        self.name = name
        self.store = store
        self.values = []

        self.count = 0
        self.max = -inf
        self.min = inf
        self._mean = 0.0
        self._m2 = 0.0

    def add(self, value):
        self.max = max(self.max, value)
        self.min = min(self.min, value)
        self.count += 1

        delta = value - self._mean
        self._mean += delta / self.count
        delta2 = value - self._mean
        self._m2 += delta * delta2

        if self.store:
            self.values.append(value)

    @property
    def sd(self):
        return sqrt(self.variance)

    @property
    def mean(self):
        return self._mean

    @property
    def variance(self):
        if self.count < 2:
            return nan

        return self._m2 / (self.count - 1)

    def __getitem__(self, idx):
        return self.values[idx]

    def report(self, f):  # pragma: no cover
        table = PrettyTable()
        table.field_names = ["Item", "Value"]
        table.align = "l"

        table.add_row(["Max", self.max])
        table.add_row(["Min", self.min])
        table.add_row(["Mean", self.mean])
        table.add_row(["Count", self.count])
        table.add_row(["Variance", self.variance])

        f.write(self.name + "\n")
        f.write(str(table))
        f.write("\n")

    def write_csv(self, f):
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["Value"])
        writer.writerows([v] for v in self.values)


class TimeSeriesSamples(Samples):
    def __init__(self, name, store=True):
        super().__init__(name, store)
        self.times = []
        self.current_time = None
        self.current_value = None

        self._ts_mean = 0.0
        self._ts_m2 = 0.0

    def add(self, time, value):
        if self.current_time is None:
            self.current_time = time
            self.current_value = value
            return
        if time <= self.current_time:
            self.current_value = value
            return

        delta = self.current_value - self._ts_mean
        w = (time - self.current_time) / time
        self._ts_mean = self._ts_mean * (1.0 - w) + self.current_value * w
        delta2 = self.current_value - self._ts_mean
        self._ts_m2 += (time - self.current_time) * delta * delta2

        if self.store:
            self.times.append(self.current_time)

        super().add(self.current_value)

        self.current_time = time
        self.current_value = value

    @property
    def mean(self):
        return self._ts_mean

    @property
    def variance(self):
        if self.count < 2:
            return nan

        return self._ts_m2 / self.current_time

    def __getitem__(self, time):
        return self.values[bisect_right(self.times, time) - 1]

    def write_csv(self, f):
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["Time", "Value"])
        writer.writerows(zip(self.times, self.values))
