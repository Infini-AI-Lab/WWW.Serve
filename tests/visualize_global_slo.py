import json
import numpy as np
import matplotlib.pyplot as plt


result_folder_centralize = "results/decentralized_test_6"
result_folder_decentralize = "results/centralized_test_6"
result_folder_single = "results/single_test_6"

file_centralize = f"{result_folder_centralize}/result.json"
file_decentralize = f"{result_folder_decentralize}/result.json"
file_single = f"{result_folder_single}/result.json"

slo_thresholds = [10*i for i in range(20, 61)]


def compute_global_slo_attainment(json_file, slo_thresholds):
    with open(json_file, "r") as f:
        results = json.load(f)

    submit_times = np.array([r["timestamp_list"][0] for r in results])
    finish_times = np.array([r["timestamp_list"][-1] for r in results])
    latencies = finish_times - submit_times

    slo_attainment = []
    for thr in slo_thresholds:
        attainment = np.sum(latencies <= thr) / len(latencies)
        slo_attainment.append(attainment * 100)

    return np.array(slo_attainment)


global_slo_centralize = compute_global_slo_attainment(file_centralize, slo_thresholds)
global_slo_decentralize = compute_global_slo_attainment(file_decentralize, slo_thresholds)
global_slo_single = compute_global_slo_attainment(file_single, slo_thresholds)



plt.figure(figsize=(5,4))

colors = {"centralize": "#D35400", "decentralize": "#2E4053", "single": "#27AE60"}
markers = {"centralize": "s", "decentralize": "o", "single": "^"}
linestyles = {"centralize": ":", "decentralize": "-.", "single": "-"}

plt.plot(slo_thresholds, global_slo_centralize, linestyle=linestyles["centralize"], linewidth=1.5, marker=markers["centralize"],
             color=colors["centralize"], markersize=2, label="Centralize")
plt.plot(slo_thresholds, global_slo_decentralize, linestyle=linestyles["decentralize"], linewidth=1.5, marker=markers["decentralize"],
             color=colors["decentralize"], markersize=2, label="Decentralize")
plt.plot(slo_thresholds, global_slo_single, linestyle=linestyles["single"], linewidth=1.5, marker=markers["single"],
             color=colors["single"], markersize=2, label="Single")

plt.xlabel("SLO Threshold (s)", fontsize=12)
plt.ylabel("SLO Attainment (%)", fontsize=12)
plt.ylim(0, 105)
plt.grid(True, linestyle=':', linewidth=1.2, alpha=0.8)
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
plt.tight_layout()
plt.savefig("slo_global.pdf", dpi=300)
