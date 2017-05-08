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



def main():
    g = nx.DiGraph(name="Milk cluster")

    # Add hosts
    for i in range(20):
        host_name = "milk{0:02d}".format(i+1)
        g.add_node(host_name, typ="host", capacity=8,
                   mac=MAC_ADDRESSES[host_name])

    for i in range(4):
        # Add edge switches
        g.add_node("edge{0}".format(i+1), typ="switch")

        # Add links between edge swtiches and hosts
        for j in range(5):
            src = "edge{0}".format(i+1)
            dst = "milk{0:02}".format(i*5+j+1)
            g.add_edge(src, dst, capacity=1)
            g.add_edge(dst, src, capacity=1)

    for i in range(2):
        # Add core switches
        g.add_node("core{0}".format(i+1), typ="switch")

        # Add links between core switches and edge switches
        for j in range(4):
            src = "core{0}".format(i+1)
            dst = "edge{0}".format(j+1)
            g.add_edge(src, dst, capacity=1)
            g.add_edge(dst, src, capacity=1)

    nx.write_graphml(g, "milk.graphml")


if __name__ == "__main__":
    main()
