import json
import sys
from logging import getLogger

import requests

import yaml

BASE_URL = "http://localhost:8080"

logger = getLogger(__name__)


def _install_unicast(dpid, src, dst, out_port):
    body = {
        "dpid": dpid,
        "match": {
            "dl_src": src,
            "dl_dst": dst,
        },
        "actions": [{
            "type": "OUTPUT",
            "port": out_port
        }]
    }
    requests.post(BASE_URL + "/stats/flowentry/add", data=json.dumps(body))


def _install_broadcast(dpid):
    body = {
        "dpid": dpid,
        "match": {
            "dl_dst": "ff:ff:ff:ff:ff:ff"
        },
        "actions": [{
            "type": "OUTPUT",
            "port": "FLOOD"
        }],
        "priority": 32768
    }
    requests.post(BASE_URL + "/stats/flowentry/add", data=json.dumps(body))


def _install_broadcast_drop(dpid, in_port):
    body = {
        "dpid": dpid,
        "match": {
            "in_port": in_port,
            "dl_dst": "ff:ff:ff:ff:ff:ff"
        },
        "priority": 32768 + 100
    }
    requests.post(BASE_URL + "/stats/flowentry/add", data=json.dumps(body))


def main():
    flowtables = {}
    path = sys.argv[1]

    with open(path) as f:
        flowtables = yaml.load(f)

    print("Starting flow installation")

    for dpid, flowtable in flowtables.items():
        print("Installing flow entries to switch {0}".format(dpid))

        print("First, clearing all existing flow entries")
        requests.delete(BASE_URL + "/stats/flowentry/clear/" + str(dpid))

        fdb = flowtable["fdb"]
        for src in fdb.keys():
            for dst, port_no in fdb[src].items():
                print("Installing flow entry for {0} -> {1}".format(src, dst))
                _install_unicast(dpid, src, dst, port_no)

        print("Installing broadcast entry")
        _install_broadcast(dpid)

        print("Installing loop avoidance entries")
        blocked_ports = flowtable["blocked_ports"]
        for port_no in blocked_ports:
            _install_broadcast_drop(dpid, port_no)

    print("All complete!")


if __name__ == "__main__":
    main()
