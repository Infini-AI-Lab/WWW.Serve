import json
import matplotlib.pyplot as plt

json_path = "/home/hywang/Reasoning/Decentralized-Agents/datasets/test_1_result.json"

with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)

t0 = min(r["timestamp_list"][0] for r in data)

timeline_data = []
for item in data:
    if item['response']["meta_data"]["finish_reason"] not in ["stop", "length"]:
        continue
    t_submit, t_start, t_end, t_return = [ts - t0 for ts in item["timestamp_list"]]
    timeline_data.append({
        "node": item['response']["source_node"],
        "request_id": item['request_id'],
        "submit": t_submit,
        "start": t_start,
        "end": t_end,
        "return": t_return
    })


colors = {
    "queue": "lightgray",
    "inference": "steelblue",
    "network": "orange"
}

fig, ax = plt.subplots(figsize=(14, 6))
y_pos = 0

for node in sorted(set(d["node"] for d in timeline_data)):
    node_data = [d for d in timeline_data if d["node"] == node]
    for req in node_data:
        # queue time
        ax.barh(y_pos, req["start"] - req["submit"], left=req["submit"], color=colors["queue"])
        # inference
        ax.barh(y_pos, req["end"] - req["start"], left=req["start"], color=colors["inference"])
        # network delay
        ax.barh(y_pos, req["return"] - req["end"], left=req["end"], color=colors["network"])
        y_pos += 1
    y_pos += 2  # 节点之间留空

ax.set_xlabel("Time (seconds)")
ax.set_ylabel("Requests")
ax.set_title("Request Timeline per Node")


plt.grid(True)
plt.tight_layout()
plt.savefig("test_1.png")