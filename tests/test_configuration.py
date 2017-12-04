from itertools import product

from pfsim.configuration import ScenarioConf


FIXTURE_SINGLE = {
    "duration": 100.0,
    "topology": "foo.graphml",
    "output": "bar",
    "algorithms": {
        "scheduler": ["FooScheduler"],
        "host_selector": ["FooHostSelector"],
        "process_mapper": ["FooProcessMapper"],
        "router": ["FooRouter"],
    },
    "jobs": [{
        "submit": {
            "distribution": "FooDistribution",
            "params": {
                "a": 1.0
            }
        },
        "duration": {
            "distribution": "BarDistribution",
            "params": {
                "b": 2.0
            }
        },
        "trace": "trace.tar.gz"
    }]
}


FIXTURE_MULTIPLE = {
    "duration": 100.0,
    "topology": "foo.graphml",
    "output": "bar",
    "algorithms": {
        "scheduler": ["FooScheduler", "BarScheduler"],
        "host_selector": ["FooHostSelector", "BarHostSelector"],
        "process_mapper": ["FooProcessMapper", "BarProcessMapper"],
        "router": ["FooRouter", "BarRouter"],
    },
    "jobs": [{
        "submit": {
            "distribution": "FooDistribution",
            "params": {
                "a": 1.0
            }
        },
        "duration": {
            "distribution": "BarDistribution",
            "params": {
                "b": 2.0
            }
        },
        "trace": "Trace"
    }]
}


class TestScenarioConf:
    def test_single(self):
        confs = list(ScenarioConf.generate_from_yaml(FIXTURE_SINGLE))

        assert len(confs) == 1

        conf = confs[0]

        assert conf.duration == 100.0
        assert conf.topology == "foo.graphml"
        assert conf.output == "bar"
        assert conf.scheduler == "FooScheduler"
        assert conf.host_selector == "FooHostSelector"
        assert conf.process_mapper == "FooProcessMapper"
        assert conf.router == "FooRouter"

        assert len(conf.jobs) == 1

        job = conf.jobs[0]

        assert job.submit_dist == "FooDistribution"
        assert job.submit_params == {"a": 1.0}
        assert job.duration_dist == "BarDistribution"
        assert job.duration_params == {"b": 2.0}
        assert job.trace == "trace.tar.gz"

    def test_multiple(self):
        confs = list(ScenarioConf.generate_from_yaml(FIXTURE_MULTIPLE))

        assert len(confs) == 16

        algo_combinations = {(conf.scheduler,
                              conf.host_selector,
                              conf.process_mapper,
                              conf.router) for conf in confs}

        algo_comninations_expected = product(
            ["FooScheduler", "BarScheduler"],
            ["FooHostSelector", "BarHostSelector"],
            ["FooProcessMapper", "BarProcessMapper"],
            ["FooRouter", "BarRouter"],
        )

        for combination in algo_comninations_expected:
            assert combination in algo_combinations
