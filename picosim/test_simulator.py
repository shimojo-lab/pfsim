from unittest.mock import MagicMock, call

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

    def test_event_ordering(self):
        _handler = MagicMock()

        self.sim.register("test", _handler)
        self.sim.schedule("test", 3.0, x="piyo")
        self.sim.schedule("test", 1.0, x="foo")
        self.sim.schedule("test", 2.0, x="hoge")
        self.sim.run()

        calls = [call(x="foo"), call(x="hoge"), call(x="piyo")]
        _handler.assert_has_calls(calls)

    def test_handler_priority(self):
        x = 0

        def _handler1():
            nonlocal x
            assert x == 0
            x += 1

        def _handler2():
            nonlocal x
            assert x == 1
            x += 1

        self.sim.register("test", _handler1, prio=1)
        self.sim.register("test", _handler2, prio=0)
        self.sim.schedule("test")
        self.sim.run()
