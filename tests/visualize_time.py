import json
import numpy as np
import matplotlib.pyplot as plt

# def compute_slo_attainment(json_file, slo_thresholds):
#     with open(json_file, "r") as f:
#         results = json.load(f)

#     submit_times = np.array([r["timestamp_list"][0] for r in results])
#     finish_times = np.array([r["timestamp_list"][-1] for r in results])
#     latencies = finish_times - submit_times

#     slo_attainment = []
#     for thr in slo_thresholds:
#         attainment = np.sum(latencies <= thr) / len(latencies)
#         slo_attainment.append(attainment)
    
#     return np.array(slo_attainment)


# file_single = "/home/hywang/Reasoning/Decentralized-Agents/results/test20/test_20_result.json"
# file_multi = "/home/hywang/Reasoning/Decentralized-Agents/results/test13/test_13_result.json"
# slo_thresholds = [20*i for i in range(0, 20)]

# slo_single = compute_slo_attainment(file_single, slo_thresholds)
# # slo_multi = compute_slo_attainment(file_multi, slo_thresholds)


# plt.figure(figsize=(5,4))

# # plt.plot(slo_thresholds, slo_multi, linestyle='-', linewidth=2, marker='o', color="#2E4053", markersize=4)
# plt.plot(slo_thresholds, slo_single, linestyle='-', linewidth=2, marker='s', color="#D35400", markersize=4)

# plt.xlabel("SLO Threshold (s)", fontsize=16)
# plt.ylabel("SLO Attainment (%)", fontsize=16)
# plt.ylim(0,1.05)
# plt.xticks(fontsize=12)
# plt.yticks(fontsize=12)
# plt.grid(True, linestyle=':', linewidth=1.5, alpha=0.8)
# plt.tight_layout()
# plt.savefig("dispatch_3_1_2.pdf", dpi=300)


def get_latency(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    latency_list = []
    error_idx = set()
    for idx, item in enumerate(data):
        if item["response"]["meta_data"]["finish_reason"] != "stop":
            error_idx.add(idx)
        latency = item["timestamp_list"][-1] - item["timestamp_list"][0]
        latency_list.append(latency)

    return latency_list, error_idx


# json_path_single = "/home/hywang/Reasoning/Decentralized-Agents/results/test18/test_18_result.json"
json_path_network = "/home/hywang/Reasoning/Decentralized-Agents/results/test23/test_23_result.json"

# latency_single, err_set_single = get_latency(json_path_single)
latency_network, err_set_network = get_latency(json_path_network)

# print(len(latency_single), len(latency_network))

# n = len(latency_single)
n = len(latency_network)
idx = np.arange(n)
bar_width = 0.4

plt.figure(figsize=(12, 6))

# plt.bar(idx - bar_width/2, latency_single, width=bar_width, 
#         label="Single", color="#1f77b4", alpha=0.8)

plt.bar(idx + bar_width/2, latency_network, width=bar_width, 
        label="DeServe", color="#ff7f0e", alpha=0.8)

plt.xlabel("Requests")
plt.ylabel("Latency (s)")
plt.legend()
plt.grid(axis="y", linestyle="--", alpha=0.7)

plt.tight_layout()
plt.savefig("time.png")
