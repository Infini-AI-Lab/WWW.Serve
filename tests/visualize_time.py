import json
import matplotlib.pyplot as plt

json_path = "/home/hywang/Reasoning/Decentralized-Agents/datasets/test_load/test_2_result.json"

with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)

times = []
done_by_list = []
route_path_cnt = {}
total_time = 0

for idx, item in enumerate(data):
    times.append(item["time_taken"])
    total_time += item["time_taken"]
    done_by_list.append(item["result"]["done_by"])
    route_path_len = len(item["result"]["route_path"])
    route_path_cnt[route_path_len] = route_path_cnt.get(route_path_len, 0) + 1

plt.figure(figsize=(14, 6))

unique_done_by = list(set(done_by_list))
colors = {name: plt.cm.tab10(i % 10) for i, name in enumerate(unique_done_by)}

for idx, (time, done_by) in enumerate(zip(times, done_by_list)):
    plt.scatter(idx, time, color=colors[done_by], s=10, label=done_by if idx == done_by_list.index(done_by) else "")


plt.xlabel("Question Index")
plt.ylabel("Time (s)")
plt.title("Request Completion Time by Done By")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("test_3.png")

print("Route path length distribution:", route_path_cnt)
print(f"Total time taken: {total_time:.2f} s, average time taken: {total_time / len(data) if data else 0:.2f} s")