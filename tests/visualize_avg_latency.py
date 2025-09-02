import json
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict

result_folder_centralize = "results/decentralized_test_6"
result_folder_decentralize = "results/centralized_test_6"
result_folder_single = "results/single_test_6"

file_centralize = f"{result_folder_centralize}/result.json"
file_decentralize = f"{result_folder_decentralize}/result.json"
file_single = f"{result_folder_single}/result.json"


def get_latency(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    latency_list = []
    source_list = []
    error_idx = set()
    for idx, item in enumerate(data):
        if item["response"]["meta_data"]["finish_reason"] not in ["stop", "length"]:
            error_idx.add(idx)
            continue
        latency = item["timestamp_list"][-1] - item["timestamp_list"][0]
        latency_list.append(latency)
        source_list.append(item["response"]["source_node"])
    return latency_list, source_list


def avg_latency_per_node(json_path):
    latency_list, source_list = get_latency(json_path)
    node_latencies = defaultdict(list)
    for lat, node in zip(latency_list, source_list):
        node_latencies[node].append(lat)
    return {node: np.mean(vals) for node, vals in node_latencies.items()}


latency_centralize = avg_latency_per_node(file_centralize)
latency_decentralize = avg_latency_per_node(file_decentralize)
latency_single = avg_latency_per_node(file_single)

all_nodes = sorted(set(latency_centralize.keys()) |
                   set(latency_decentralize.keys()) |
                   set(latency_single.keys()))

centralize_vals = [latency_centralize.get(n, np.nan) for n in all_nodes]
decentralize_vals = [latency_decentralize.get(n, np.nan) for n in all_nodes]
single_vals = [latency_single.get(n, np.nan) for n in all_nodes]


x = np.arange(len(all_nodes))
width = 0.25

plt.figure(figsize=(8,5))
bars1 = plt.bar(x - width, centralize_vals, width, label="Centralize", color="#D35400")
bars2 = plt.bar(x, decentralize_vals, width, label="Decentralize", color="#2E4053")
bars3 = plt.bar(x + width, single_vals, width, label="Single", color="#27AE60")

plt.ylabel("Avg Latency (s)", fontsize=12)
plt.xticks(x, all_nodes, rotation=45, ha="right", fontsize=9)
plt.legend()
plt.grid(axis="y", linestyle=":", linewidth=1.2, alpha=0.7)

plt.tight_layout()
plt.savefig("avg_latency_per_node.pdf", dpi=300)
