import json
import numpy as np
import matplotlib.pyplot as plt


result_folder_centralize = "results/decentralized_test_10"
result_folder_decentralize = "results/centralized_test_10"
result_folder_single = "results/single_test_10"

file_centralize = f"{result_folder_centralize}/result.json"
file_decentralize = f"{result_folder_decentralize}/result.json"
file_single = f"{result_folder_single}/result.json"


slo_thresholds = [10*i for i in range(10, 30)]

def compute_slo_attainment(json_file, slo_thresholds):
    with open(json_file, "r") as f:
        results = json.load(f)

    all_slo = []
    for node in ["node1", "node2", "node3", "node4"]:
        submit_times = np.array([r["timestamp_list"][0] for r in results if r["response"]["source_node"] == node])
        finish_times = np.array([r["timestamp_list"][-1] for r in results if r["response"]["source_node"] == node])
        latencies = finish_times - submit_times

        slo_attainment = []
        for thr in slo_thresholds:
            attainment = np.sum(latencies <= thr) / len(latencies)
            slo_attainment.append(attainment)

        all_slo.append(slo_attainment)

    return np.array(all_slo)


all_slo_centralize = compute_slo_attainment(file_centralize, slo_thresholds)
all_slo_decentralize = compute_slo_attainment(file_decentralize, slo_thresholds)
all_slo_single = compute_slo_attainment(file_single, slo_thresholds)



plt.figure(figsize=(20, 4))

nodes = ["node1", "node2", "node3", "node4"]
colors = {"centralize": "#D35400", "decentralize": "#2E4053", "single": "#27AE60"}
markers = {"centralize": "s", "decentralize": "o", "single": "^"}
linestyles = {"centralize": ":", "decentralize": "-.", "single": "-"}


for i, node in enumerate(nodes, start=1):
    plt.subplot(1, 4, i)

    slo_centralize = all_slo_centralize[i-1]
    slo_decentralize = all_slo_decentralize[i-1]
    slo_single = all_slo_single[i-1]

    plt.plot(slo_thresholds, slo_centralize,
             linestyle=linestyles["centralize"], linewidth=1.5, marker=markers["centralize"],
             color=colors["centralize"], markersize=2, label="Centralize")
    plt.plot(slo_thresholds, slo_single,
             linestyle=linestyles["single"], linewidth=1.5, marker=markers["single"],
             color=colors["single"], markersize=2, label="Single")
    plt.plot(slo_thresholds, slo_decentralize,
             linestyle=linestyles["decentralize"], linewidth=1.5, marker=markers["decentralize"],
             color=colors["decentralize"], markersize=2, label="Decentralize")

    plt.title(node, fontsize=14)
    plt.xlabel("SLO Threshold (s)", fontsize=12)
    if i == 1:
        plt.ylabel("SLO Attainment (%)", fontsize=12)
    plt.ylim(0, 1.05)
    plt.grid(True, linestyle=':', linewidth=1.2, alpha=0.8)
    plt.xticks(fontsize=10)
    plt.yticks(fontsize=10)
    if i == 4:
        plt.legend(fontsize=10)

plt.tight_layout()
plt.savefig("all_slo.pdf", dpi=300)
