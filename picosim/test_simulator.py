from unittest.mock import MagicMock

from .simulator import Simulator


class TestSimulator:
    def setup(self):
        self.sim = Simulator()

    def test_empty(self):
        self.sim.run()

    def test_register(self):
        _handler = MagicMock()

        self.sim.register("test", _handler)
        self.sim.schedule("test")
        self.sim.run()

        _handler.assert_called_once_with()

    def test_handler_args(self):
        _handler = MagicMock()

        self.sim.register("test", _handler)
        self.sim.schedule("test", foo=1, hoge="test")
        self.sim.run()

        _handler.assert_called_once_with(foo=1, hoge="test")

    def test_time_progression(self):
        assert self.sim.time == 0.0

        self.sim.schedule("test", time=123.4)
        self.sim.run()

        assert self.sim.time == 123.4
