import json
import sys
import tarfile

import matplotlib.pyplot as plt

import numpy as np


def main():
    trace_archive = sys.argv[1]
    traffic_matrix = {}

    with tarfile.open(trace_archive, "r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile() or not member.name.endswith(".json"):
                continue

            with tar.extractfile(member) as f:
                trace = json.load(f)
                rank = trace["rank"]
                tx_bytes = trace["tx_bytes"]

                traffic_matrix[rank] = tx_bytes

    n_procs = len(traffic_matrix)
    a = np.ndarray([n_procs, n_procs])
    for src, vec in traffic_matrix.items():
        for dst, traffic in enumerate(vec):
            a[src][dst] = traffic

    plt.pcolor(a, cmap="Blues")
    plt.colorbar()
    plt.show()


if __name__ == "__main__":
    main()
