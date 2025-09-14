import _setup_path
import asyncio
from openai import AsyncOpenAI
import json
import os
import time
import aiohttp
from prometheus_client.parser import text_string_to_metric_families
from typing import Dict, List, Optional, Tuple


async def _get_server_metrics(
    server_url: str,
    metric_list: Optional[List[str]] = None
) -> Optional[List[Dict]]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{server_url}/metrics", timeout=3) as response:
                if response.status != 200:
                    return None
                metrics_text = await response.text()

        parsed_metrics = []
        for family in text_string_to_metric_families(metrics_text):
            for sample in family.samples:
                if metric_list is None or sample.name in metric_list:
                    parsed_metrics.append({
                        "name": sample.name,
                        "value": sample.value,
                        "labels": sample.labels
                    })
        return parsed_metrics

    except Exception as e:
        return None


async def get_server_metrics(server_url: str, is_sglang: bool) -> Tuple[int, int, float]:
    """Get server metrics for the model."""
    if is_sglang:
        metrics = await _get_server_metrics(server_url, metric_list=
                                            ["sglang:num_running_reqs",
                                            "sglang:num_queue_reqs",
                                            "sglang:token_usage"])
    else:
        metrics = await _get_server_metrics(server_url, metric_list=
                                            ["vllm:num_requests_running",
                                            "vllm:num_requests_waiting",
                                            "vllm:gpu_cache_usage_perc"])

    if metrics is None:
        return 999, 999, 1.0

    raw_metrics = {entry["name"]: entry["value"] for entry in metrics}

    if is_sglang:
        token_usage = raw_metrics.get("sglang:token_usage", 1.0)
        num_running_reqs = raw_metrics.get("sglang:num_running_reqs", 999)
        num_queue_reqs = raw_metrics.get("sglang:num_queue_reqs", 999)
    else:
        token_usage = raw_metrics.get("vllm:gpu_cache_usage_perc", 1.0)
        num_running_reqs = raw_metrics.get("vllm:num_requests_running", 999)
        num_queue_reqs = raw_metrics.get("vllm:num_requests_waiting", 999)

    return int(num_running_reqs), int(num_queue_reqs), token_usage


class RecordStats:
    def __init__(self, server_url, is_sglang):
        self.stats = []
        self.server_url = server_url
        self.is_sglang = is_sglang
        asyncio.create_task(self.record_stats_periodically())

    async def record_stats_periodically(self):
        while True:
            num_running_reqs, num_queue_reqs, token_usage = await get_server_metrics(server_url=self.server_url, is_sglang=self.is_sglang)
            self.stats.append({
                "timestamp": time.time(),
                "num_running_reqs": num_running_reqs,
                "num_queue_reqs": num_queue_reqs,
                "token_usage": token_usage
            })
            await asyncio.sleep(3)


async def inference_request(idx, client_name, client, model_path, problem):
    submit_time = time.time()
    try:
        meta_response = await client.chat.completions.create(
            model = model_path,
            messages = [{
                "role": "user",
                "content": problem
            }],
            extra_body={
                "chat_template_kwargs": {"enable_thinking": True},
            },
            temperature = 0.0,
            top_p = 0.95,
            max_tokens = 8192
        )
        finish_time = time.time()

        response = {
            "idx": idx,
            "timestamp_list": [submit_time, finish_time],
            "response": {
                "content": meta_response.choices[0].message.content,
                "meta_data": {
                    "finish_reason": meta_response.choices[0].finish_reason,
                    "usage": {
                        "prompt_tokens": meta_response.usage.prompt_tokens,
                        "completion_tokens": meta_response.usage.completion_tokens,
                        "total_tokens": meta_response.usage.total_tokens
                    }
                },
                "source_node": client_name,
                "executor_node": client_name
            }
        }
    except Exception as e:
        print(f"Error dispatching request {idx}: {e}")
        response = {
            "idx": idx,
            "timestamp_list": [submit_time, 0],
            "response": {
                "content": "Error occurred",
                "meta_data": {
                    "finish_reason": "error",
                    "usage": {
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0
                    }
                },
                "source_node": client_name,
                "executor_node": client_name
            }
        }

    return response


async def submit_with_delay(idx, client_name, client, model_path, problem, delay):
    await asyncio.sleep(delay)
    print(f"Submitting request {idx} to {client_name}")
    return await inference_request(idx, client_name, client, model_path, problem)


NODES_INFO = {
    "node1": {
        "base_url": "http://192.168.102.11:30000/v1/",
        "api_key": "None",
        "model_path": "Qwen/Qwen3-32B",
        "is_sglang": True
    },
    "node2": {
        "base_url": "http://192.168.102.20:30001/v1/",
        "api_key": "None",
        "model_path": "Qwen/Qwen3-8B",
        "is_sglang": True
    },
    "node3": {
        "base_url": "http://192.168.102.12:30002/v1/",
        "api_key": "None",
        "model_path": "/home/hywang/Reasoning/Decentralized-Agents/models/deepseek-ai--DeepSeek-R1-Distill-Qwen-7B",
        "is_sglang": False
    },
    "node4": {
        "base_url": "http://192.168.102.19:30003/v1/",
        "api_key": "None",
        "model_path": "/home/hywang/Reasoning/Decentralized-Agents/models/meta-llama--Llama-3.1-8B",
        "is_sglang": False
    }
}


async def main():
    clients = {
        name: (AsyncOpenAI(base_url=info["base_url"], api_key=info["api_key"]), info["model_path"])
        for name, info in NODES_INFO.items()
    }
    await asyncio.sleep(1)
    stats = {
        name: RecordStats(info["base_url"][:-4], is_sglang=info["is_sglang"])
        for name, info in NODES_INFO.items()
    }
    await asyncio.sleep(1)

    ##### Testing code #####
    with open("results/poisson_times.json", "r", encoding="utf-8") as f:
        poisson_times = json.load(f)

    tasks = [
        asyncio.create_task(submit_with_delay(item["idx"], item["target"], clients[item["target"]][0], clients[item["target"]][1], item["problem"], item["delay"]))
        for item in poisson_times
    ]
    all_results = await asyncio.gather(*tasks)

    result_folder = "results/single_test_4/"
    os.makedirs(result_folder, exist_ok=True)

    with open(f"{result_folder}/result.json", "w", encoding="utf-8") as f:
        json.dump(
            all_results,
            f,
            ensure_ascii=False,
            indent=4,
        )

    for node, record_stats in stats.items():
        node_stats = record_stats.stats
        with open(f"{result_folder}/{node}.json", "w", encoding="utf-8") as f:
            json.dump(
                {node: node_stats},
                f,
                ensure_ascii=False,
                indent=4,
            )


if __name__ == "__main__":
    asyncio.run(main())