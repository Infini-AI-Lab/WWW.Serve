import json
import matplotlib.pyplot as plt
import pandas as pd

def load_json_as_df(json_path, end = 9999):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    data_list = list(data.values())[0]
    start_time = data_list[0]['timestamp']
    df = pd.DataFrame(data_list)
    df['relative_time'] = df['timestamp'] - start_time
    df = df[df['relative_time'] <= end]
    return df


json_path_single = "/home/hywang/Reasoning/Decentralized-Agents/results/test8/test_8_node_3.json"
json_path_network = "/home/hywang/Reasoning/Decentralized-Agents/results/test7/test_7_node_3.json"

json_path_network_others = [
    "/home/hywang/Reasoning/Decentralized-Agents/results/test7/test_7_node_1.json",
    "/home/hywang/Reasoning/Decentralized-Agents/results/test7/test_7_node_2.json",
    # "/home/hywang/Reasoning/Decentralized-Agents/results/test3/test_7_node_4.json",
]

df_single = load_json_as_df(json_path_single)
df_network = load_json_as_df(json_path_network)
dfs_network_others = [load_json_as_df(p) for p in json_path_network_others]

# load_key = "num_running_reqs"
load_key = "token_usage"

plt.figure(figsize=(12, 6))

plt.plot(df_single['relative_time'], df_single[load_key],
            color="orange", linewidth=2.5, label="Target Node (Single)")

plt.plot(df_network['relative_time'], df_network[load_key],
            color="blue", linewidth=2.5, linestyle="--", label="Target Node (DeServe)")

for df_other in dfs_network_others:
    plt.plot(df_other['relative_time'], df_other[load_key],
                color="gray", linewidth=1, alpha=0.3)


plt.xlabel("Relative Time (s)")
plt.ylabel(load_key.replace("_", " ").title())
plt.legend()
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig("load.png")
