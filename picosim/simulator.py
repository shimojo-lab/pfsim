from collections import defaultdict
from heapq import heappop, heappush
from logging import getLogger
from operator import itemgetter

logger = getLogger(__name__)


class Event:
    def __init__(self, name, time, data={}):
        self.name = name
        self.time = time
        self.data = data

    def __repr__(self):
        return "<Event {0} t:{1} data:{2}>".format(self.name, self.time,
                                                   self.data)

    def __lt__(self, other):
        return self.time < other.time


class Simulator:
    def __init__(self):
        self.event_queue = []
        self.event_handlers = defaultdict(list)
        self.time = 0.0
        self.n_events = 0

        self.schedule("simulator.started")

    def run(self):
        while self.event_queue:
            self.step()

    def run_until(self, time):
        while self.event_queue and self.event_queue[0][0] <= time:
            self.step()

    def step(self):
        if not self.event_queue:
            return

        self.time, ev = heappop(self.event_queue)
        self.n_events += 1

        if ev.name not in self.event_handlers:
            logger.warning("No handler is registered for {0}".format(ev.name))

        for prio, handler in self.event_handlers[ev.name]:
            handler(**ev.data)

    def schedule_after(self, name, time=None, **kwargs):
        self.schedule(name, self.time + time, **kwargs)

    def schedule(self, name, time=None, **kwargs):
        if not time:
            time = self.time

        ev = Event(name, time, kwargs)
        heappush(self.event_queue, (ev.time, ev))

    def register(self, name, handler, prio=0):
        handlers = self.event_handlers[name]

        handlers.append((prio, handler))
        handlers.sort(key=itemgetter(0), reverse=True)
