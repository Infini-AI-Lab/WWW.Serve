import json
import numpy as np
import matplotlib.pyplot as plt


result_folder_centralize = "results/decentralized_test_4"
result_folder_decentralize = "results/centralized_test_4"
result_folder_single = "results/single_test_4"

file_centralize = f"{result_folder_centralize}/result.json"
file_decentralize = f"{result_folder_decentralize}/result.json"
file_single = f"{result_folder_single}/result.json"

# file_1 = "results/decentralized_test_8_1/result.json"
# file_2 = "results/decentralized_test_8_2/result.json"
# file_3 = "results/decentralized_test_8_3/result.json"
# file_4 = "results/decentralized_test_8_4/result.json"


slo_thresholds = [10*i for i in range(20, 46)]


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

# global_slo_1 = compute_global_slo_attainment(file_1, slo_thresholds)
# global_slo_2 = compute_global_slo_attainment(file_2, slo_thresholds)
# global_slo_3 = compute_global_slo_attainment(file_3, slo_thresholds)
# global_slo_4 = compute_global_slo_attainment(file_4, slo_thresholds)


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

# plt.plot(slo_thresholds, global_slo_1, linestyle=':', linewidth=1.5, marker='o',
#              color='#2E4053', markersize=2, label="Decentralized-1")
# plt.plot(slo_thresholds, global_slo_2, linestyle='-.', linewidth=1.5, marker='s',
#              color='#D35400', markersize=2, label="Decentralized-2")
# plt.plot(slo_thresholds, global_slo_3, linestyle='--', linewidth=1.5, marker='^',
#              color='#27AE60', markersize=2, label="Decentralized-3")
# plt.plot(slo_thresholds, global_slo_4, linestyle='-', linewidth=1.5, marker='x',
#              color='#8E44AD', markersize=2, label="Decentralized-4")


plt.xlabel("SLO Threshold (s)", fontsize=12)
plt.ylabel("SLO Attainment (%)", fontsize=12)
plt.ylim(40, 105)
plt.grid(True, linestyle=':', linewidth=1.2, alpha=0.8)
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
plt.tight_layout()
plt.savefig("slo_global_4.pdf", dpi=300)
