from collections import defaultdict
from heapq import heappop, heappush
from logging import getLogger

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

    def run(self):
        while self.event_queue:
            self.time, ev = heappop(self.event_queue)
            self.n_events += 1

            logger.info("Event {0} at {1}".format(ev.name, self.time))

            for handler in self.event_handlers[ev.name]:
                handler(**ev.data)

    def schedule(self, name, time=None, **kwargs):
        if not time:
            time = self.time

        ev = Event(name, time, kwargs)
        heappush(self.event_queue, (ev.time, ev))

    def register(self, name, handler):
        self.event_handlers[name].append(handler)
