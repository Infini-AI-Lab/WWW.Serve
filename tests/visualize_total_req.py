import json
import pandas as pd
import matplotlib.pyplot as plt

# result_folder = "results/decentralized_test_1"
result_folder = "results/centralized_test_1"

files = [
    f"{result_folder}/node1.json",
    f"{result_folder}/node2.json",
    f"{result_folder}/node3.json",
    f"{result_folder}/node4.json"
]


dfs = []
for f in files:
    with open(f, "r") as fp:
        data = json.load(fp)
    data = list(data.values())[0]
    df = pd.DataFrame(data)
    df = df[["timestamp", "num_running_reqs"]]
    dfs.append(df)

all_times = sorted(set().union(*[set(df["timestamp"]) for df in dfs]))
t0 = min(all_times)
all_times = [t - t0 for t in all_times]

timeline = pd.DataFrame(index=all_times)

for i, df in enumerate(dfs):
    df["rel_time"] = df["timestamp"] - t0
    df = df.set_index("rel_time").sort_index()
    df_reindexed = df.reindex(timeline.index, method="ffill").fillna(0)
    timeline[f"node{i+1}"] = df_reindexed["num_running_reqs"]

timeline["total_running"] = timeline.sum(axis=1)

plt.figure(figsize=(12,6))
plt.step(timeline.index, timeline["total_running"], where="post", linewidth=2.0, color="#211388")

# plt.axvline(x=200, color="green", linestyle="--", linewidth=1)
# plt.text(200, timeline["total_running"].max()*0.3, "Node 4 Left", 
#          rotation=90, color="green", va="center", ha="right", fontsize=16)

# plt.axvline(x=400, color="green", linestyle="--", linewidth=1)
# plt.text(400, timeline["total_running"].max()*0.3, "Node 3 Left", 
#          rotation=90, color="green", va="center", ha="right", fontsize=16)

# plt.axvline(x=600, color="red", linestyle="--", linewidth=1)
# plt.text(600, timeline["total_running"].max()*0.3, "Dispatch stopped", 
#          rotation=90, color="red", va="center", ha="right", fontsize=16)

# plt.axvline(x=900, color="red", linestyle="--", linewidth=1)
# plt.text(900, timeline["total_running"].max()*0.3, "Dispatch stopped", 
#          rotation=90, color="red", va="center", ha="right", fontsize=16)

plt.xticks(fontsize=12)
plt.yticks(fontsize=12)
plt.xlabel("Time (s)", fontsize=16)
plt.ylabel("Number of Total Running Requests", fontsize=16)
plt.grid(True, linestyle=':', linewidth=1.5, alpha=0.8)
plt.savefig("total_running_requests.pdf", dpi=300)