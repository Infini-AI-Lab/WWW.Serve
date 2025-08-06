import json
import matplotlib.pyplot as plt


json_files = {
    # "datasets/test_results_sglang.json": "SGLang 7B",
    "datasets/test_results_sglang_1.5B.json": "SGLang 1.5B",
    # "datasets/test_results_s_7B_1.5B.json": "SGLang 7B + SGLang 1.5B",
    "datasets/test_results_s_1.5B_2.json": "SGLang 1.5B (100)",
}


color_list = plt.get_cmap("Dark2").colors
color_index = 0


plt.figure(figsize=(14, 6))


for file, label in json_files.items():
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)

        for node in ["node1", "node2", "node3"]:
            time_series = [
                (idx, item["time_taken"])
                for idx, item in enumerate(data)
                if "time_taken" in item and item.get("result", {}).get("done_by") == node
            ]
            if time_series:
                x_vals, y_vals = zip(*time_series)
                plt.scatter(
                    x_vals,
                    y_vals,
                    label=f"{label} - {node}",
                    s=10,
                    alpha=0.8,
                    color=color_list[color_index % len(color_list)],
                )
                color_index += 1



plt.title("Inference Time Comparison")
plt.xlabel("Question Index")
plt.ylabel("Time (s)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("inference_time_comparison.png")
