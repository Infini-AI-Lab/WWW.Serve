#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt

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
req_per_min = df['TotalTokens'].resample('1min').count()
tokens_per_min = df['TotalTokens'].resample('1min').sum()

# 6. Plot request count over time
plt.figure(figsize=(12, 6))
plt.plot(req_per_min.index, req_per_min.values, label='Requests per Minute', color='blue')
plt.xlabel("Time")
plt.ylabel("Number of Requests")
plt.title("LLM Inference Requests Over Time")
plt.grid(True)
plt.legend()
plt.savefig("LLM_Inference_Requests_Over_Time.png")

# 7. Plot total tokens per minute
plt.figure(figsize=(12, 6))
plt.plot(tokens_per_min.index, tokens_per_min.values, label='Total Tokens per Minute', color='orange')
plt.xlabel("Time")
plt.ylabel("Total Tokens")
plt.title("Total Tokens Processed Over Time")
plt.grid(True)
plt.legend()
plt.savefig("LLM_Inference_Tokens_Over_Time.png")
