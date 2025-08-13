# Azure LLM Inference Trace Visualization & Node Workload Generation

## 1. Download the **Azure LLM Inference Trace** dataset from Azure Blob storage.

The dataset is publicly available:

**URL**:  
https://azurepublicdatasettraces.blob.core.windows.net/azurellminfererencetrace/AzureLLMInferenceTrace_conv_1week.csv
Download it to your working directory:

```bash
wget -O AzureLLMInferenceTrace_conv_1week.csv \
  "https://azurepublicdatasettraces.blob.core.windows.net/azurellminfererencetrace/AzureLLMInferenceTrace_conv_1week.csv"
```

## 2. Visualize the workload distribution over time
```bash
python vizualize_workload.py
```

## 3. Generate per-node workloads with configurable time offsets and sampling
```bash
python generate_workload_for_nodes.py
```