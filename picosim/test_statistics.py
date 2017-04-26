from math import isclose

from .statistics import TimedSamples


class TestTimedSamples:
    def setup(self):
        self.samples = TimedSamples("Test Timed Samples")

    def test_empty(self):
        assert self.samples.mean == 0.0
        assert self.samples.count == 0

    def test_single(self):
        self.samples.add(0.0, 1.0)
        self.samples.add(1.0, 2.0)

        assert self.samples.mean == 1.0
        assert self.samples.variance == 0.0
        assert self.samples.count == 1
        assert self.samples.max == 1.0
        assert self.samples.min == 1.0

    def test_multiple(self):
        self.samples.add(0.0, 1.0)
        self.samples.add(1.0, 2.0)
        self.samples.add(3.0, 3.0)
        self.samples.add(6.0, 4.0)

        mean = (1.0 * 1.0 + 2.0 * 2.0 + 3.0 * 3.0) / (1.0 + 2.0 + 3.0)
        variance = (1.0 * (1.0 - mean) ** 2 + 2.0 * (2.0 - mean) ** 2
                    + 3.0 * (3.0 - mean) ** 2) / (1.0 + 2.0 + 3.0)

        assert isclose(self.samples.mean, mean)
        assert isclose(self.samples.variance, variance)
        assert self.samples.count == 3
        assert self.samples.max == 3.0
        assert self.samples.min == 1.0

    def test_duplicate_time(self):
        self.samples.add(0.0, 1.0)
        self.samples.add(0.0, 2.0)
        self.samples.add(1.0, 3.0)

        assert self.samples.mean == 2.0
        assert self.samples.variance == 0.0
        assert self.samples.count == 1
        assert self.samples.max == 2.0
        assert self.samples.min == 2.0
