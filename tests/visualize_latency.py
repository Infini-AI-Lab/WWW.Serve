import json
import numpy as np
import matplotlib.pyplot as plt


result_folder = "results/decentralized_test_19"
# result_folder = "results/centralized_test_19"
# result_folder = "results/single_test_19"

json_path = f"{result_folder}/result.json"

def get_latency(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    latency_list = []
    source_list = []
    executor_list = []
    error_idx = set()
    for idx, item in enumerate(data):
        if item["response"]["meta_data"]["finish_reason"] not in ["stop", "length"]:
            error_idx.add(idx)
            continue
        latency = item["timestamp_list"][-1] - item["timestamp_list"][0]
        latency_list.append(latency)
        executor_list.append(item["response"]["executor_node"])
        source_list.append(item["response"]["source_node"])

    return latency_list, executor_list, source_list, error_idx

latency, executor_list, source_list, err_set = get_latency(json_path)

n = len(latency)
idx = np.arange(n)

colors = {
    "node1": "#A0A0A0",
    "node2": "#7FB0C0",
    "node3": "#B0C070",
    "node4": "#D95F02"
}

plt.figure(figsize=(12, 6))

for i, (req_id, lat, node) in enumerate(zip(idx, latency, executor_list)):
    plt.bar(req_id, lat, color=colors[node], label=f"Node {node[-1]}" if i == executor_list.index(node) else "", width=0.7)

plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.xlabel("Request Index", fontsize=16)
plt.ylabel("Latency (s)", fontsize=16)
plt.savefig("latency.pdf", dpi=300)
