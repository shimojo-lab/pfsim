import json
import sys
import tarfile

from collections import defaultdict

import matplotlib.pyplot as plt

import numpy as np

import seaborn as sns


def plot_traffic_matrix(traffic_matrix, path):
    fig, ax = plt.subplots()
    cmap = sns.cubehelix_palette(dark=0, light=1, as_cmap=True)
    img = ax.imshow(traffic_matrix, cmap=cmap)
    cb = fig.colorbar(img)
    cb.set_label("Sent Bytes")
    ax.set_xlabel("Sender Rank")
    ax.set_ylabel("Receiver Rank")
    ax.grid(b=False)
    fig.savefig(path, bbox_inches="tight")


def plot_message_matrix(message_matrix, path):
    fig, ax = plt.subplots()
    cmap = sns.cubehelix_palette(dark=0, light=1, as_cmap=True)
    img = ax.imshow(message_matrix, cmap=cmap)
    cb = fig.colorbar(img)
    cb.set_label("Sent Messages")
    ax.set_xlabel("Sender Rank")
    ax.set_ylabel("Receiver Rank")
    ax.grid(b=False)
    fig.savefig(path, bbox_inches="tight")


def plot_message_size_histogram(message_sizes, path):
    fig, ax = plt.subplots()

    values = list(message_sizes.keys())
    weights = list(message_sizes.values())

    ax.hist(values, bins=50, weights=weights)
    ax.set_xlabel("Message Size [B]")
    ax.set_ylabel("Messages Sent")
    fig.savefig(path, bbox_inches="tight")


def main():
    sns.set(context="paper", rc={"font.size": 10})

    trace_archive = sys.argv[1]
    traffic_matrix = None
    message_matrix = None
    message_sizes = defaultdict(lambda: 0)

    with tarfile.open(trace_archive, "r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile() or not member.name.endswith(".json"):
                continue

            with tar.extractfile(member) as f:
                trace = json.load(f)
                n_procs = trace["n_procs"]

                if traffic_matrix is None:
                    traffic_matrix = np.zeros((n_procs, n_procs))
                    message_matrix = np.zeros((n_procs, n_procs))

                rank = trace["rank"]

                traffic_matrix[rank] = trace["tx_bytes"]
                message_matrix[rank] = trace["tx_messages"]

                for item in trace["tx_message_sizes"]:
                    message_sizes[item["message_size"]] += item["frequency"]

    plot_traffic_matrix(traffic_matrix, "traffic_matrix.pdf")
    plot_message_matrix(message_matrix, "message_matrix.pdf")
    plot_message_size_histogram(message_sizes, "message_size_histogram.pdf")


if __name__ == "__main__":
    main()
