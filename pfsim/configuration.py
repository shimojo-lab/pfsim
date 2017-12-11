from collections import namedtuple
from itertools import product

from schema import Or, Schema


EXPERIMENT_CONF_SCHEMA = Schema({
    "duration": Or(int, float),
    "topology": str,
    "output": str,
    "algorithms": {
        "scheduler": [str],
        "host_selector": [str],
        "process_mapper": [str],
        "router": [str]
    },
    "jobs": [{
        "submit": {
            "distribution": str,
            "params": {
                str: Or(str, int, float)
            }
        },
        "duration": {
            "distribution": str,
            "params": {
                str: Or(str, int, float)
            }
        },
        "trace": str
    }]
})


_JobConf = namedtuple(
    "JobConf",
    [
        "submit_dist",
        "submit_params",
        "duration_dist",
        "duration_params",
        "trace"
    ]
)


class JobConf(_JobConf):
    pass


_Scenario = namedtuple(
    "Scenario",
    [
        "duration",
        "topology",
        "output",
        "scheduler",
        "host_selector",
        "process_mapper",
        "router",
        "jobs"
    ]
)


class Scenario(_Scenario):
    @classmethod
    def generate_from_yaml(cls, yml):
        d = EXPERIMENT_CONF_SCHEMA.validate(yml)

        schedulers = d["algorithms"]["scheduler"]
        host_selectors = d["algorithms"]["host_selector"]
        process_mappers = d["algorithms"]["process_mapper"]
        routers = d["algorithms"]["router"]

        algorithms = product(schedulers, host_selectors, process_mappers,
                             routers)

        jobs = [JobConf(
                submit_dist=jd["submit"]["distribution"],
                submit_params=jd["submit"]["params"],
                duration_dist=jd["duration"]["distribution"],
                duration_params=jd["duration"]["params"],
                trace=jd["trace"]) for jd in d["jobs"]]

        for (sched, hs, pm, rt) in algorithms:
            yield cls(duration=d["duration"],
                      topology=d["topology"],
                      output=d["output"],
                      scheduler=sched,
                      host_selector=hs,
                      process_mapper=pm,
                      router=rt,
                      jobs=jobs)
