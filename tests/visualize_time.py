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
    executor_list = []
    error_idx = set()
    for idx, item in enumerate(data):
        # if item["response"]["meta_data"]["finish_reason"] == "TIMEOUT":
        #     error_idx.add(idx)
        #     continue
        latency = item["timestamp_list"][-1] - item["timestamp_list"][0]
        latency_list.append(latency)
        executor_list.append(item["response"]["executor_node"])

    return latency_list, executor_list, error_idx


json_path_network = "/home/hywang/Reasoning/Decentralized-Agents/results/test35/test_35_result.json"

latency_network, executor_list, err_set_network = get_latency(json_path_network)

n = len(latency_network)
idx = np.arange(n)

colors = {
    "node1": "#A0A0A0",  # 浅灰
    "node2": "#7FB0C0",  # 浅灰蓝
    "node3": "#B0C070",  # 浅灰绿
    "node4": "#D95F02"   # 暖橙，重点
}



plt.figure(figsize=(12, 6))

for i, (req_id, lat, node) in enumerate(zip(idx, latency_network, executor_list)):
    plt.bar(req_id, lat, color=colors[node], label=f"Node {node[-1]}" if i == executor_list.index(node) else "", width=0.7)

# plt.legend(fontsize=16)
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.xlabel("Request Index", fontsize=16)
plt.ylabel("Latency (s)", fontsize=16)
plt.savefig("time.pdf", dpi=300)
