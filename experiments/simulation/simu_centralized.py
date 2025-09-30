import asyncio
from openai import AsyncOpenAI
import json
import os
import time
import aiohttp
from prometheus_client.parser import text_string_to_metric_families
from typing import Dict, List, Optional, Tuple
import yaml
from pathlib import Path


# Only support single model per node for now
def _parse_node_config(config_path) -> Dict:
    with open(config_path, "r", encoding="utf-8") as f:
        node_cfg = yaml.safe_load(f)
    return {
        "base_url": node_cfg["models"][0]["base_url"],
        "api_key": node_cfg["models"][0].get("api_key", "None"),
        "model_path": node_cfg["models"][0]["model_path"],
        "is_sglang": node_cfg["server_params"]["policy"] == "default_sglang",
    }


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


async def update_all_server_stats(clients):
    stats = {}
    for name, (client, _, is_sglang) in clients.items():
        try:
            server_url = str(client.base_url)[:-4]  # .../v1/
            num_running, num_queued, token_usage = await get_server_metrics(server_url, is_sglang=is_sglang)
            stats[name] = {
                "num_running_reqs": num_running,
                "num_queue_reqs": num_queued,
                "token_usage": token_usage,
            }
        except Exception as e:
            print(f"Error updating stats for {name}: {e}")
            stats[name] = {
                "num_running_reqs": 999,
                "num_queue_reqs": 999,
                "token_usage": 1.0,
            }
    return stats


def choose_server(stats, source):
    if source in stats and stats[source]["token_usage"] < 0.6:
        return source
    return min(stats, key=lambda k: (stats[k]["token_usage"], stats[k]["num_running_reqs"] + stats[k]["num_queue_reqs"]))


async def dispatch_request(idx, clients, source, problem):
    submit_time = time.time()

    stats = await update_all_server_stats(clients)
    target_server = choose_server(stats, source)
    for name, stat in stats.items():
        print(f"{name}: {stat}")
    print(f"Dispatching request {idx} to {target_server}")
    client = clients[target_server][0]
    model_path = clients[target_server][1]

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
            "server_stats": stats,
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
                "source_node": source,
                "executor_node": target_server
            }
        }
    except Exception as e:
        print(f"Error dispatching request {idx}: {e}")
        response = {
            "idx": idx,
            "timestamp_list": [submit_time, 0],
            "server_stats": stats,
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
                "source_node": source,
                "executor_node": target_server
            }
        }

    return response


async def run_with_delay(idx, clients, target, problem, delay):
    await asyncio.sleep(delay)
    return await dispatch_request(idx, clients, target, problem)

##### Load node configurations #####
CONFIG_PATH = Path(__file__).parent.parent.parent / "node_configs"
NODES_INFO = {
    "node1": _parse_node_config(CONFIG_PATH / "node1.yaml"),
    "node2": _parse_node_config(CONFIG_PATH / "node2.yaml"),
    "node3": _parse_node_config(CONFIG_PATH / "node3.yaml"),
    "node4": _parse_node_config(CONFIG_PATH / "node4.yaml"),
}
print("Loaded node configurations:", NODES_INFO)


async def main():
    clients = {
        name: (AsyncOpenAI(base_url=f"{info["base_url"]}/v1", api_key=info["api_key"]), info["model_path"], info["is_sglang"])
        for name, info in NODES_INFO.items()
    }
    await asyncio.sleep(1)

    ##### Start recording stats #####
    stats = {
        name: RecordStats(info["base_url"], is_sglang=info["is_sglang"])
        for name, info in NODES_INFO.items()
    }
    await asyncio.sleep(1)

    ##### Load input requests #####
    input_path = Path(__file__).parent / "simu_input.json"
    with open(input_path, "r", encoding="utf-8") as f:
        poisson_times = json.load(f)

    tasks = [
        asyncio.create_task(run_with_delay(item["idx"], clients, item["target"], item["problem"], item["delay"])) for item in poisson_times
    ]
    all_results = await asyncio.gather(*tasks)

    ##### Save results #####
    RESULT_PATH = Path(__file__).parent.parent / "results"
    result_folder = RESULT_PATH / f"centralized_simulation"
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