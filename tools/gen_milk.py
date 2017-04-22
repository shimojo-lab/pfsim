import networkx as nx


def main():
    g = nx.DiGraph(name="Milk cluster")

    # Add hosts
    for i in range(20):
        g.add_node("milk{0:02d}".format(i+1), typ="host", capacity=8)

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
