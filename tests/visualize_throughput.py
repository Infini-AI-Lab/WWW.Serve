import json
import numpy as np
import matplotlib.pyplot as plt


result_folder_centralize = "results/decentralized_test_4"
result_folder_decentralize = "results/centralized_test_4"
result_folder_single = "results/single_test_4"

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
    latency = (finish_times - submit_times).mean()
    throughput = num_requests / total_time

    return throughput, latency


tp_centralize, latency_centralize = compute_throughput(file_centralize)
tp_decentralize, latency_decentralize = compute_throughput(file_decentralize)
tp_single, latency_single = compute_throughput(file_single)

plt.figure(figsize=(4,4))

settings = ["Centralize", "Decentralize", "Single"]
tps = [tp_centralize, tp_decentralize, tp_single]

bars = plt.bar(settings, tps, color=["#D35400", "#2E4053", "#27AE60"], width=0.4)

for bar, val in zip(bars, tps):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f"{val:.3f}", 
             ha='center', va='bottom', fontsize=12)

plt.ylabel("Throughput (req/s)", fontsize=12)
plt.ylim(0.3, 0.38)
plt.grid(axis="y", linestyle=":", linewidth=1.2, alpha=0.8)
plt.xticks(fontsize=12)
plt.yticks(fontsize=10)
plt.tight_layout()
plt.savefig("throughput.pdf", dpi=300)


# plt.figure(figsize=(2.5,4))
# tps = [tp_centralize, tp_decentralize, tp_single]
# settings = ["Centralize", "Decentralize", "Single"]
# colors = ["#D35400", "#2E4053", "#27AE60"]

# plt.scatter([0]*len(tps), tps, s=120, color=colors)
# for i, (y, label) in enumerate(zip(tps, settings)):
#     plt.text(0.05, y, f"{label}\n{y:.3f}", va="center", fontsize=10)

# plt.xlim(-0.5, 0.8)
# plt.xticks([])
# plt.ylabel("Throughput (req/s)", fontsize=12)
# plt.grid(axis="y", linestyle=":", alpha=0.7)
# plt.tight_layout()
# plt.savefig("throughput_vertical.pdf", dpi=300)

