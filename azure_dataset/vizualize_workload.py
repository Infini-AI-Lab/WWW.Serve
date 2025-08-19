#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# 1. Load CSV
file_path = "AzureLLMInferenceTrace_conv_1week.csv"
df = pd.read_csv(file_path)

# 2. Parse TIMESTAMP to datetime
df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'],format='mixed')

# 3. Calculate total tokens processed per request
df['TotalTokens'] = df['ContextTokens'] + df['GeneratedTokens']

# 4. Set timestamp as index for resampling
df.set_index('TIMESTAMP', inplace=True)

# 5. Resample to minute/hour for smoother curves (change '1min' to '1H' if needed)
req_per_hour = df['TotalTokens'].resample('1h').count()
# tokens_per_hour = df['TotalTokens'].resample('1h').sum()


plt.figure(figsize=(9, 5))
ax = plt.gca()

ax.plot(req_per_hour.index, req_per_hour.values,
        color="#26467B", linewidth=1.4, label="Requests per Hour")

ax.set_ylabel("Requests per Hour", fontsize=16, fontweight='bold')

ax.yaxis.set_major_formatter(
    ticker.FuncFormatter(lambda x, _: f'{int(x/1e3)}K')
)

noons = pd.date_range(start=req_per_hour.index.min().normalize() + pd.Timedelta(hours=12),
                      end=req_per_hour.index.max().normalize() + pd.Timedelta(hours=12),
                      freq="1D")

ax.set_xticks(noons)
ax.set_xticklabels([d.strftime("%m-%d") for d in noons],
                   fontsize=16, fontweight='bold')

ax.tick_params(axis="y", labelsize=12)

ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.7)


for spine in ax.spines.values():
    spine.set_linewidth(0.3)
    spine.set_color("#444444")

plt.tight_layout()
plt.savefig("azure.pdf", bbox_inches="tight")
plt.close()


# # 6. Plot request count over time
# plt.figure(figsize=(12, 6))
# plt.plot(req_per_min.index, req_per_min.values, label='Requests per Hour', color='blue')
# plt.ylabel("Number of Requests")
# plt.grid(True)
# plt.legend()
# plt.savefig("temp.png")

# 7. Plot total tokens per minute
# plt.figure(figsize=(12, 6))
# plt.plot(tokens_per_min.index, tokens_per_min.values, label='Total Tokens per Minute', color='orange')
# plt.xlabel("Time")
# plt.ylabel("Total Tokens")
# plt.title("Total Tokens Processed Over Time")
# plt.grid(True)
# plt.legend()
# plt.savefig("LLM_Inference_Tokens_Over_Time.png")
