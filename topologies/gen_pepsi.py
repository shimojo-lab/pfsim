import networkx as nx


def main():
    g = nx.DiGraph(name="Pepsi cluster")

    # Add hosts
    for i in range(28):
        g.add_node("pepsi{0:02d}".format(i+1), typ="host", capacity=12)

    for i in range(4):
        # Add edge switches
        g.add_node("edge{0}".format(i+1), typ="switch")

        # Add links between edge swtiches and hosts
        for j in range(7):
            src = "edge{0}".format(i+1)
            dst = "pepsi{0:02}".format(i*7+j+1)
            g.add_edge(src, dst, capacity=1)
            g.add_edge(dst, src, capacity=1)

    for i in range(2):
        # Add core switches
        g.add_node("core{0}".format(i+1), typ="switch")

        # Add links between core switches and edge switches
        for j in range(4):
            src = "core{0}".format(i+1)
            dst = "edge{0}".format(j+1)
            g.add_edge(src, dst, capacity=4)
            g.add_edge(dst, src, capacity=4)

    nx.write_graphml(g, "pepsi.graphml")


if __name__ == "__main__":
    main()
