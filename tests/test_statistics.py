from math import isclose, isnan
from io import StringIO

from pfsim.statistics import Samples, TimeSeriesSamples


class TestTimedSamples:
    def setup(self):
        self.samples = TimeSeriesSamples("Test Timed Samples")

    def test_empty(self):
        assert self.samples.mean == 0.0
        assert isnan(self.samples.variance)
        assert self.samples.count == 0

    def test_single(self):
        self.samples.add(0.0, 1.0)
        self.samples.add(1.0, 2.0)

        assert self.samples.mean == 1.0
        assert isnan(self.samples.variance)
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
        assert isnan(self.samples.variance)
        assert self.samples.count == 1
        assert self.samples.max == 2.0
        assert self.samples.min == 2.0

    def test_get_item(self):
        self.samples.add(0.0, 1.0)
        self.samples.add(1.0, 2.0)
        self.samples.add(2.0, 3.0)

        assert self.samples[0.0] == 1.0
        assert self.samples[1.0] == 2.0
        assert self.samples[0.5] == 1.0
        assert self.samples[1.5] == 2.0

    def test_write_csv(self):
        self.samples.add(0.0, 1.0)
        self.samples.add(1.0, 2.0)
        self.samples.add(3.0, 3.0)
        self.samples.add(6.0, 4.0)

        with StringIO() as f:
            self.samples.write_csv(f)

            assert f.getvalue() == "Time,Value\n"\
                                   "0.0,1.0\n"\
                                   "1.0,2.0\n"\
                                   "3.0,3.0\n"


class TestSamples:
    def setup(self):
        self.samples = Samples("Test Samples")

    def test_empty(self):
        assert self.samples.mean == 0.0
        assert isnan(self.samples.variance)
        assert self.samples.count == 0

    def test_single(self):
        self.samples.add(1.0)

        assert self.samples.mean == 1.0
        assert isnan(self.samples.variance)
        assert self.samples.count == 1
        assert self.samples.max == 1.0
        assert self.samples.min == 1.0

    def test_multiple(self):
        self.samples.add(1.0)
        self.samples.add(2.0)
        self.samples.add(3.0)

        assert self.samples.mean == 2.0
        assert self.samples.variance == 1.0
        assert self.samples.count == 3
        assert self.samples.max == 3.0
        assert self.samples.min == 1.0

    def test_get_item(self):
        self.samples.add(1.0)
        self.samples.add(2.0)
        self.samples.add(3.0)

        assert self.samples[0] == 1.0
        assert self.samples[1] == 2.0
        assert self.samples[2] == 3.0

    def test_write_csv(self):
        self.samples.add(1.0)
        self.samples.add(2.0)
        self.samples.add(3.0)

        with StringIO() as f:
            self.samples.write_csv(f)

            assert f.getvalue() == "Value\n"\
                                   "1.0\n"\
                                   "2.0\n"\
                                   "3.0\n"
