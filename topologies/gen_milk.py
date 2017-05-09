import networkx as nx

MAC_ADDRESSES = {
    "milk01": "00:30:48:cf:f0:39",
    "milk02": "00:30:48:cf:f0:37",
    "milk03": "00:30:48:cd:a5:61",
    "milk04": "00:30:48:cf:f2:37",
    "milk05": "00:30:48:cf:f0:c7",
    "milk06": "00:30:48:f2:61:f5",
    "milk07": "00:30:48:cf:c1:15",
    "milk08": "00:30:48:cf:f0:35",
    "milk09": "00:30:48:cf:f0:a7",
    "milk10": "00:30:48:f2:62:35",
    "milk11": "00:30:48:cf:f0:a1",
    "milk12": "00:30:48:cf:f2:35",
    "milk13": "00:30:48:cf:f0:97",
    "milk14": "00:30:48:cf:f0:33",
    "milk15": "00:30:48:cf:f0:9f",
    "milk16": "00:30:48:cf:f0:d1",
    "milk17": "00:30:48:cf:f0:ab",
    "milk18": "00:30:48:f1:65:77",
    "milk19": "00:30:48:cf:f0:1b",
    "milk20": "00:30:48:cf:f2:33"
}

DPIDS = {
        "core1": 1,
        "core2": 2,
        "edge1": 3,
        "edge2": 4,
        "edge3": 5,
        "edge4": 6,
}

PORT_NUMS = {
    "core1": {
        "edge1": 1,
        "edge2": 3,
        "edge3": 5,
        "edge4": 7
    },
    "core2": {
        "edge1": 41,
        "edge2": 43,
        "edge3": 45,
        "edge4": 47
    },
    "edge1": {
        "core1": 9,
        "core2": 11,
        "milk01": 10,
        "milk02": 12,
        "milk03": 14,
        "milk04": 16,
        "milk05": 15,
    },
    "edge2": {
        "core1": 17,
        "core2": 19,
        "milk06": 18,
        "milk07": 21,
        "milk08": 20,
        "milk09": 22,
        "milk10": 24
    },
    "edge3": {
        "core1": 25,
        "core2": 27,
        "milk11": 26,
        "milk12": 28,
        "milk13": 29,
        "milk14": 30,
        "milk15": 32
    },
    "edge4": {
        "core1": 33,
        "core2": 35,
        "milk16": 37,
        "milk17": 34,
        "milk18": 36,
        "milk19": 38,
        "milk20": 40
    },
}


def main():
    g = nx.DiGraph(name="Milk cluster")

    # Add hosts
    for i in range(20):
        host_name = "milk{0:02d}".format(i+1)
        g.add_node(host_name, typ="host", capacity=8,
                   mac=MAC_ADDRESSES[host_name])

    for i in range(4):
        # Add edge switches
        sw_name = "edge{0}".format(i+1)
        g.add_node(sw_name, typ="switch", dpid=DPIDS[sw_name])

        # Add links between edge swtiches and hosts
        for j in range(5):
            src = "edge{0}".format(i+1)
            dst = "milk{0:02}".format(i*5+j+1)
            g.add_edge(src, dst, capacity=1, port=PORT_NUMS[src][dst])
            g.add_edge(dst, src, capacity=1)

    for i in range(2):
        # Add core switches
        sw_name = "core{0}".format(i+1)
        g.add_node(sw_name, typ="switch", dpid=DPIDS[sw_name])

        # Add links between core switches and edge switches
        for j in range(4):
            src = "core{0}".format(i+1)
            dst = "edge{0}".format(j+1)
            g.add_edge(src, dst, capacity=1, port=PORT_NUMS[src][dst])
            g.add_edge(dst, src, capacity=1, port=PORT_NUMS[dst][src])

    nx.write_graphml(g, "milk.graphml")


if __name__ == "__main__":
    main()
