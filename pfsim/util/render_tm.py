import json
import sys
import tarfile

from collections import defaultdict

import matplotlib.pyplot as plt

import numpy as np

import seaborn as sns


width = 3.487
height = width / 1.618


def plot_matrix(message_matrix, path, title):
    fig, ax = plt.subplots()
    fig.subplots_adjust(left=.05, bottom=.18, top=.90)

    cmap = sns.cubehelix_palette(dark=0, light=1, as_cmap=True)
    img = ax.imshow(message_matrix, cmap=cmap)
    cb = fig.colorbar(img)
    cb.set_label(title)
    ax.set_xlabel("Sender Rank")
    ax.set_ylabel("Receiver Rank")
    ax.grid(b=False)

    fig.set_size_inches(width, height)
    fig.savefig(path)


def plot_traffic_matrix(traffic_matrix, path):
    plot_matrix(traffic_matrix, path, "Sent Bytes")


def plot_message_matrix(message_matrix, path):
    plot_matrix(message_matrix, path, "Sent Messages")


def plot_message_size_histogram(message_sizes, path):
    fig, ax = plt.subplots()
    fig.subplots_adjust(left=.27, bottom=.20, right=.88, top=.97)

    values = list(message_sizes.keys())
    weights = list(message_sizes.values())

    ax.hist(values, bins=50, weights=weights)
    ax.set_xlabel("Message Size [B]")
    ax.set_ylabel("Messages Sent")

    fig.set_size_inches(width, height)
    fig.savefig(path)


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
