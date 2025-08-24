import json
import numpy as np
import matplotlib.pyplot as plt


result_folder_centralize = "results/decentralized_test_1"
result_folder_decentralize = "results/centralized_test_1"

file_centralize = f"{result_folder_centralize}/result.json"
file_decentralize = f"{result_folder_decentralize}/result.json"
slo_thresholds = [20*i for i in range(0, 20)]

def compute_slo_attainment(json_file, slo_thresholds):
    with open(json_file, "r") as f:
        results = json.load(f)

    submit_times = np.array([r["timestamp_list"][0] for r in results])
    finish_times = np.array([r["timestamp_list"][-1] for r in results])
    latencies = finish_times - submit_times

    slo_attainment = []
    for thr in slo_thresholds:
        attainment = np.sum(latencies <= thr) / len(latencies)
        slo_attainment.append(attainment)
    
    return np.array(slo_attainment)


slo_centralize = compute_slo_attainment(file_centralize, slo_thresholds)
slo_decentralize = compute_slo_attainment(file_decentralize, slo_thresholds)


plt.figure(figsize=(5,4))

plt.plot(slo_thresholds, slo_centralize, linestyle='-', linewidth=2, marker='s', color="#D35400", markersize=4)
plt.plot(slo_thresholds, slo_decentralize, linestyle='-', linewidth=2, marker='o', color="#2E4053", markersize=4)

plt.xlabel("SLO Threshold (s)", fontsize=16)
plt.ylabel("SLO Attainment (%)", fontsize=16)
plt.ylim(0,1.05)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.grid(True, linestyle=':', linewidth=1.5, alpha=0.8)
plt.tight_layout()
plt.savefig("slo.pdf", dpi=300)