import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.patches as mpatches

colors = {
    "node1": "#A0A0A0",  # 浅灰
    "node2": "#7FB0C0",  # 浅灰蓝
    "node3": "#B0C070",  # 浅灰绿
    "node4": "#D95F02"   # 暖橙，重点
}

# colors = ["#5A9BD5", "#ED7D31", "#70AD47"]
# colors = ["#5A9BD5", "#E38BB1"]
# single_color_1 = "#211388"
# network_color_1 = "#F61414"

# single_color_1 = "#7030A0"
# network_color_1 = "#008080"

# single_color_2 = "#2E4053"
# network_color_2 = "#D35400"

handles = []
labels = []

handles = [
    # mlines.Line2D([], [], color=single_color_1, linestyle='-', marker='s', markersize=4, linewidth=2),
    # mlines.Line2D([], [], color=network_color_1, linestyle='-', marker='o', markersize=4, linewidth=2),
    # mlines.Line2D([], [], color=single_color_2, linestyle='-', marker='s', markersize=4, linewidth=2),
    # mlines.Line2D([], [], color=network_color_2, linestyle='-', marker='o', markersize=4, linewidth=2),
    # mlines.Line2D([], [], color=colors[0], linewidth=2.0),
    # mlines.Line2D([], [], color=colors[1], linewidth=2.0),
    # mlines.Line2D([], [], color=colors[2], linewidth=2.0),
    mpatches.Patch(color=colors["node1"], label="Node 1"),
    mpatches.Patch(color=colors["node2"], label="Node 2"),
    mpatches.Patch(color=colors["node3"], label="Node 3"),
    mpatches.Patch(color=colors["node4"], label="Node 4"),
]

labels = [
    "Node 1",
    "Node 2",
    "Node 3",
    "Node 4",
    # "Node 1 (Single)",
    # "Node 1 (WWW.Serve)",
    # "Node 2 (Single)",
    # "Node 2 (WWW.Serve)",
    # "Node 3 (WWW.Serve)",
    # "Node 4 (WWW.Serve)",
]



fig, ax = plt.subplots(figsize=(12,1))
ax.axis("off")

legend = ax.legend(
    handles, labels, 
    loc="center left",
    bbox_to_anchor=(-0.15, 0.6),
    frameon=False, 
    handlelength=2.0, 
    handletextpad=1.0,
    fontsize=16,
    labelspacing=2.0,    # 调整行间距，默认是0.5
    handleheight=1.2     # 每行句柄高度，可稍微拉开
)

# legend = ax.legend(
#     handles, labels, 
#     loc="upper center",       # 横向 legend 常用上方居中
#     bbox_to_anchor=(0.492, 0.8),# 调整整体位置
#     frameon=False, 
#     handlelength=2.0, 
#     handletextpad=1.0,
#     fontsize=16,
#     labelspacing=0.5,         # 横向 legend 不需要太大行间距
#     handleheight=1.2,
#     ncol=len(handles)          # 横向排列每条线占一列
# )

plt.savefig("stake_0.pdf", dpi=300)
