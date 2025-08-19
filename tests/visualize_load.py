import json
import numpy as np
import matplotlib.pyplot as plt


single_node_json_1 = "/home/hywang/Reasoning/Decentralized-Agents/results/test23/test_23_node_4.json"
# network_node_json_1 = "/home/hywang/Reasoning/Decentralized-Agents/results/test13/test_13_node_1.json"

# single_node_json_3 = "/home/hywang/Reasoning/Decentralized-Agents/results/test14/test_14_node_3.json"
# network_node_json_3 = "/home/hywang/Reasoning/Decentralized-Agents/results/test13/test_13_node_3.json"

other_nodes_json = [
    "/home/hywang/Reasoning/Decentralized-Agents/results/test23/test_23_node_1.json",
    "/home/hywang/Reasoning/Decentralized-Agents/results/test23/test_23_node_2.json",
    "/home/hywang/Reasoning/Decentralized-Agents/results/test23/test_23_node_3.json"
]


def load_model_data(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)
    data = list(data.values())[0]
    time = []
    running = []
    token = []
    for item in data:
        # if item["num_running_reqs"] == 0:
        #     continue
        time.append(item["timestamp"])
        running.append(item["num_running_reqs"])
        token.append(item["token_usage"]*100)
    start_time = time[0]
    time = [t - start_time for t in time]

    return time, running, token


time_single_1, running_single_1, token_single_1 = load_model_data(single_node_json_1)
# time_single_1 = [t + 150 for t in time_single_1]
# time_single_3, running_single_3, token_single_3 = load_model_data(single_node_json_3)
# time_network_1, running_network_1, token_network_1 = load_model_data(network_node_json_1)
# time_network_3, running_network_3, token_network_3 = load_model_data(network_node_json_3)

# max_len = len(time_network_3)

# time_single_1 = time_single_1[:max_len]
# running_single_1 = running_single_1[:max_len]
# token_single_1 = token_single_1[:max_len]

# time_single_3 = time_single_3[:max_len]
# running_single_3 = running_single_3[:max_len]
# token_single_3 = token_single_3[:max_len]


other_times = []
other_runnings = []
other_tokens = []
for f in other_nodes_json:
    t, r, token = load_model_data(f)
    other_times.append(t)
    other_runnings.append(r)
    other_tokens.append(token)


plt.figure(figsize=(6,4))

colors = ["#5A9BD5", "#ED7D31", "#70AD47"]
# colors = ["#5A9BD5", "#E38BB1"]
for (t, r), c in zip(zip(other_times, other_runnings), colors):
    plt.plot(t, r, color=c, linewidth=1.0, linestyle='-')

plt.plot(time_single_1, running_single_1, linestyle='-', linewidth=2.0, marker='s', color="#7030A0", markersize=0) # Single
# plt.plot(time_network_1, running_network_1, linestyle='-', linewidth=2.0, marker='o', color="#008080", markersize=0) # Network

# plt.plot(time_single_3, running_single_3, linestyle='-', linewidth=2.0, marker='s', color="#2E4053", markersize=0) # Single
# plt.plot(time_network_3, running_network_3, linestyle='-', linewidth=2.0, marker='o', color="#D35400", markersize=0) # Network


plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.xlabel("Time (s)", fontsize=16)
plt.ylabel("Number of Running Requests", fontsize=16)
plt.grid(True, linestyle=':', linewidth=1.5, alpha=0.8)
plt.tight_layout()
plt.savefig("dispatch_3_2_2.pdf", dpi=300)




plt.figure(figsize=(6,4))

for (t, r), c in zip(zip(other_times, other_tokens), colors):
    plt.plot(t, r, color=c, linewidth=1.0, linestyle='-')

plt.plot(time_single_1, token_single_1, linestyle='-', linewidth=2.0, marker='s', color="#7030A0", markersize=0) # Single
# plt.plot(time_network_1, token_network_1, linestyle='-', linewidth=2.0, marker='o', color="#008080", markersize=0) # Network

# plt.plot(time_single_3, token_single_3, linestyle='-', linewidth=2.0, marker='s', color="#2E4053", markersize=0) # Single
# plt.plot(time_network_3, token_network_3, linestyle='-', linewidth=2.0, marker='o', color="#D35400", markersize=0) # Network


plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.xlabel("Time (s)", fontsize=16)
plt.ylabel("Token Usage (%)", fontsize=16)
plt.grid(True, linestyle=':', linewidth=1.5, alpha=0.8)
plt.tight_layout()
plt.savefig("dispatch_3_3_2.pdf", dpi=300)



# def load_json_as_df(json_path, end = 9999):
#     with open(json_path, "r", encoding="utf-8") as f:
#         data = json.load(f)
#     data_list = list(data.values())[0]
#     start_time = data_list[0]['timestamp']
#     df = pd.DataFrame(data_list)
#     df['relative_time'] = df['timestamp'] - start_time
#     df = df[df['relative_time'] <= end]
#     return df


# json_path_single = "/home/hywang/Reasoning/Decentralized-Agents/results/test18/test_18_node_1.json"
# json_path_network = "/home/hywang/Reasoning/Decentralized-Agents/results/test17/test_17_node_1.json"

# json_path_network_others = [
#     "/home/hywang/Reasoning/Decentralized-Agents/results/test17/test_17_node_2.json",
#     "/home/hywang/Reasoning/Decentralized-Agents/results/test17/test_17_node_3.json",
#     "/home/hywang/Reasoning/Decentralized-Agents/results/test17/test_17_node_4.json",
# ]

# df_single = load_json_as_df(json_path_single)
# df_network = load_json_as_df(json_path_network)
# dfs_network_others = [load_json_as_df(p) for p in json_path_network_others]

# # load_key = "num_running_reqs"
# load_key = "token_usage"

# plt.figure(figsize=(12, 6))

# plt.plot(df_single['relative_time'], df_single[load_key],
#             color="orange", linewidth=2.5, label="Target Node (Single)")

# plt.plot(df_network['relative_time'], df_network[load_key],
#             color="blue", linewidth=2.5, linestyle="--", label="Target Node (DeServe)")

# for df_other in dfs_network_others:
#     plt.plot(df_other['relative_time'], df_other[load_key],
#                 color="gray", linewidth=1, alpha=0.3)


# plt.xlabel("Relative Time (s)")
# plt.ylabel(load_key.replace("_", " ").title())
# plt.legend()
# plt.grid(True, linestyle="--", alpha=0.5)
# plt.tight_layout()
# plt.savefig("load.png")
