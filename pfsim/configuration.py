from collections import namedtuple
from itertools import product

from schema import Or, Schema


EXPERIMENT_CONF_SCHEMA = Schema({
    "duration": Or(int, float),
    "topology": [{
        "kind": str,
        "params": {
            str: object
        }
    }],
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


_TopologyConf = namedtuple(
    "TopologyConf",
    [
        "kind",
        "params",
    ]
)


class TopologyConf(_TopologyConf):
    pass


_Scenario = namedtuple(
    "Scenario",
    [
        "id",
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

        topologies = d["topology"]
        schedulers = d["algorithms"]["scheduler"]
        host_selectors = d["algorithms"]["host_selector"]
        process_mappers = d["algorithms"]["process_mapper"]
        routers = d["algorithms"]["router"]

        algorithms = product(topologies, schedulers, host_selectors,
                             process_mappers, routers)

        jobs = [JobConf(
                submit_dist=jd["submit"]["distribution"],
                submit_params=jd["submit"]["params"],
                duration_dist=jd["duration"]["distribution"],
                duration_params=jd["duration"]["params"],
                trace=jd["trace"]) for jd in d["jobs"]]

        for i, (topo, sched, hs, pm, rt) in enumerate(algorithms):
            yield cls(id=i,
                      duration=d["duration"],
                      topology=TopologyConf(kind=topo["kind"],
                                            params=topo["params"]),
                      output=d["output"],
                      scheduler=sched,
                      host_selector=hs,
                      process_mapper=pm,
                      router=rt,
                      jobs=jobs)
