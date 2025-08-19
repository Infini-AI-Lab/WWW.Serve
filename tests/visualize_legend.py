import matplotlib.pyplot as plt
import matplotlib.lines as mlines

colors = ["#5A9BD5", "#ED7D31", "#70AD47"]
# colors = ["#5A9BD5", "#E38BB1"]
single_color_1 = "#211388"
network_color_1 = "#F61414"

# single_color_1 = "#7030A0"
# network_color_1 = "#008080"

# single_color_2 = "#2E4053"
# network_color_2 = "#D35400"

handles = []
labels = []

handles = [
    mlines.Line2D([], [], color=single_color_1, linestyle='-', marker='s', markersize=4, linewidth=2),
    mlines.Line2D([], [], color=network_color_1, linestyle='-', marker='o', markersize=4, linewidth=2),
    # mlines.Line2D([], [], color=single_color_2, linestyle='-', marker='s', markersize=4, linewidth=2),
    # mlines.Line2D([], [], color=network_color_2, linestyle='-', marker='o', markersize=4, linewidth=2),
    mlines.Line2D([], [], color=colors[0], linewidth=2.0),
    mlines.Line2D([], [], color=colors[1], linewidth=2.0),
    mlines.Line2D([], [], color=colors[2], linewidth=2.0),
]

labels = [
    "Node 1 (Single)",
    "Node 1 (DeServe)",
    # "Node 2 (Single)",
    "Node 2 (DeServe)",
    "Node 3 (DeServe)",
    "Node 4 (DeServe)",
    # "Node 4 (DeServe)",
]



fig, ax = plt.subplots(figsize=(20,1))
ax.axis("off")

# legend = ax.legend(
#     handles, labels, 
#     loc="center left",
#     bbox_to_anchor=(-0.15, 0.6),
#     frameon=False, 
#     handlelength=2.0, 
#     handletextpad=1.0,
#     fontsize=16,
#     labelspacing=2.0,    # 调整行间距，默认是0.5
#     handleheight=1.2     # 每行句柄高度，可稍微拉开
# )

legend = ax.legend(
    handles, labels, 
    loc="upper center",       # 横向 legend 常用上方居中
    bbox_to_anchor=(0.5, 0.8),# 调整整体位置
    frameon=False, 
    handlelength=2.0, 
    handletextpad=1.0,
    fontsize=16,
    labelspacing=0.5,         # 横向 legend 不需要太大行间距
    handleheight=1.2,
    ncol=len(handles)          # 横向排列每条线占一列
)

plt.savefig("dispatch_1_0.pdf", dpi=300)
