import json
import matplotlib.pyplot as plt


# json_files = {
#     # "datasets/test_results_sglang.json": "SGLang 7B",
#     "datasets/test_results_sglang_1.5B.json": "SGLang 1.5B",
#     # "datasets/test_results_s_7B_1.5B.json": "SGLang 7B + SGLang 1.5B",
#     "datasets/test_results_s_1.5B_2.json": "SGLang 1.5B (100)",
# }


# color_list = plt.get_cmap("Dark2").colors
# color_index = 0


# plt.figure(figsize=(14, 6))


# for file, label in json_files.items():
#     with open(file, "r", encoding="utf-8") as f:
#         data = json.load(f)

#         for node in ["node1", "node2", "node3"]:
#             time_series = [
#                 (idx, item["time_taken"])
#                 for idx, item in enumerate(data)
#                 if "time_taken" in item and item.get("result", {}).get("done_by") == node
#             ]
#             if time_series:
#                 x_vals, y_vals = zip(*time_series)
#                 plt.scatter(
#                     x_vals,
#                     y_vals,
#                     label=f"{label} - {node}",
#                     s=10,
#                     alpha=0.8,
#                     color=color_list[color_index % len(color_list)],
#                 )
#                 color_index += 1




json_path = "/home/hywang/Reasoning/Decentralized-Agents/datasets/test_3.json"

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

# plt.figure(figsize=(14, 6))

# unique_done_by = list(set(done_by_list))
# colors = {name: plt.cm.tab10(i % 10) for i, name in enumerate(unique_done_by)}

# for idx, (time, done_by) in enumerate(zip(times, done_by_list)):
#     plt.scatter(idx, time, color=colors[done_by], s=10, label=done_by if idx == done_by_list.index(done_by) else "")


# plt.xlabel("Question Index")
# plt.ylabel("Time (s)")
# plt.title("Request Completion Time by Done By")
# plt.legend()
# plt.grid(True)
# plt.tight_layout()
# plt.savefig("test_3.png")

print("Route path length distribution:", route_path_cnt)
print(f"Total time taken: {total_time:.2f} s, average time taken: {total_time / len(data) if data else 0:.2f} s")