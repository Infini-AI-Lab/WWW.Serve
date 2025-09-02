import json
import numpy as np
import matplotlib.pyplot as plt


result_folder_centralize = "results/decentralized_test_15"
result_folder_decentralize = "results/centralized_test_15"
result_folder_single = "results/single_test_15"

file_centralize = f"{result_folder_centralize}/result.json"
file_decentralize = f"{result_folder_decentralize}/result.json"
file_single = f"{result_folder_single}/result.json"


def compute_throughput(json_file):
    with open(json_file, "r") as f:
        results = json.load(f)

    submit_times = np.array([r["timestamp_list"][0] for r in results if r["response"]["meta_data"]["finish_reason"] in ["stop", "length"]])
    finish_times = np.array([r["timestamp_list"][-1] for r in results if r["response"]["meta_data"]["finish_reason"] in ["stop", "length"]])

    num_requests = len(submit_times)
    total_time = finish_times.max() - submit_times.min()
    throughput = num_requests / total_time

    return throughput


tp_centralize = compute_throughput(file_centralize)
tp_decentralize = compute_throughput(file_decentralize)
tp_single = compute_throughput(file_single)

print("centralized: ", tp_centralize)
print("decentralized: ", tp_decentralize)
print("single: ", tp_single)

plt.figure(figsize=(5,4))

# hatches = ["//", "\\\\", "xx"]
settings = ["Centralize", "Decentralize", "Single"]
tps = [tp_centralize, tp_decentralize, tp_single]

bars = plt.bar(settings, tps, color=["#D35400", "#2E4053", "#27AE60"], width=0.4)

for bar, val in zip(bars, tps):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f"{val:.3f}", 
             ha='center', va='bottom', fontsize=12)

plt.ylabel("Throughput (req/s)", fontsize=12)
plt.ylim(0.1, 0.4)
plt.grid(axis="y", linestyle=":", linewidth=1.2, alpha=0.8)
plt.xticks(fontsize=12)
plt.yticks(fontsize=10)
plt.tight_layout()
plt.savefig("throughput.pdf", dpi=300)
