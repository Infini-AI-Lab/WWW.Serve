import json
import matplotlib.pyplot as plt

# visualize_item = "num_running_reqs"
visualize_item = "token_usage"

# result_folder = "results/decentralized_test_1"
# result_folder = "results/centralized_test_4"
result_folder = "results/single_test_1"

other_nodes_json = [
    f"{result_folder}/node1.json",
    f"{result_folder}/node2.json",
    f"{result_folder}/node3.json",
    f"{result_folder}/node4.json"
]


def load_model_data(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)
    data = list(data.values())[0]
    time = []
    running = []
    token = []
    for item in data:
        time.append(item["timestamp"])
        running.append(item["num_running_reqs"])
        token.append(item["token_usage"]*100)
    start_time = time[0]
    time = [t - start_time for t in time]

    time = [t for t in time if t < 1100]
    len_time = len(time)
    running = running[:len_time]
    token = token[:len_time]

    return time, running, token


times = []
runnings = []
usages = []
for idx, f in enumerate(other_nodes_json):
    t, r, token = load_model_data(f)

    times.append(t)
    runnings.append(r)
    usages.append(token)


plt.figure(figsize=(12,6))

colors = ["#A0A0A0", "#7FB0C0", "#B0C070", "#D95F02"]

if visualize_item == "num_running_reqs":
    for t, r, c in zip(times, runnings, colors):
        plt.plot(t, r, color=c, linewidth=1.5, linestyle='-')
    plt.ylabel("Number of Running Requests", fontsize=16)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.xlabel("Time (s)", fontsize=16)
    plt.grid(True, linestyle=':', linewidth=1.5, alpha=0.8)
    plt.tight_layout()
    plt.savefig("running_reqs.pdf", dpi=300)
elif visualize_item == "token_usage":
    for t, u, c in zip(times, usages, colors):
        plt.plot(t, u, color=c, linewidth=1.5, linestyle='-')
    plt.ylim(0, 105)
    plt.ylabel("Token Usage (%)", fontsize=16)
    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)
    plt.xlabel("Time (s)", fontsize=16)
    plt.grid(True, linestyle=':', linewidth=1.5, alpha=0.8)
    plt.tight_layout()
    plt.savefig("token_usage.pdf", dpi=300)
