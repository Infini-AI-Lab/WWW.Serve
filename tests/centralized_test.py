import _setup_path
import asyncio
from openai import AsyncOpenAI
import json
import os
import time
from typing import Dict, List, Optional, Tuple
import aiohttp
from prometheus_client.parser import text_string_to_metric_families


async def _get_sglang_metrics(
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


async def get_server_metrics(server_url: str) -> Tuple[int, int, float]:
    """Get server metrics for the model."""
    metrics = await _get_sglang_metrics(server_url, metric_list=
                                        ["sglang:num_running_reqs",
                                        "sglang:num_queue_reqs",
                                        "sglang:token_usage"])

    if metrics is None:
        return 999, 999, 1.0

    raw_metrics = {entry["name"]: entry["value"] for entry in metrics}

    token_usage = raw_metrics.get("sglang:token_usage", 1.0)
    num_running_reqs = raw_metrics.get("sglang:num_running_reqs", 999)
    num_queue_reqs = raw_metrics.get("sglang:num_queue_reqs", 999)

    return int(num_running_reqs), int(num_queue_reqs), token_usage


async def update_all_server_stats(clients):
    stats = {}
    for name, (client, _) in clients.items():
        try:
            server_url = str(client.base_url)[:-4]  # .../v1/
            num_running, num_queued, token_usage = await get_server_metrics(server_url)
            stats[name] = {
                "running": num_running,
                "queued": num_queued,
                "usage": token_usage,
            }
        except Exception as e:
            print(f"Error updating stats for {name}: {e}")
            stats[name] = {
                "running": 999,
                "queued": 999,
                "usage": 1.0,
            }
    return stats


def choose_server(stats):
    return min(stats, key=lambda k: (stats[k]["usage"], stats[k]["running"] + stats[k]["queued"]))


async def dispatch_request(idx, clients, source, problem):
    submit_time = time.time()

    stats = await update_all_server_stats(clients)
    target_server = choose_server(stats)
    print(f"Server stats: {stats}")
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
            temperature = 0.6,
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
            "timestamp_list": [submit_time, -1],
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
                "executor": target_server
            }
        }

    return response


async def run_with_delay(idx, clients, target, problem, delay):
    await asyncio.sleep(delay)
    return await dispatch_request(idx, clients, target, problem)


NODES_INFO = {
    "node1": {
        "base_url": "http://192.168.102.12:30000/v1",
        "api_key": "None",
        "model_path": "Qwen/Qwen3-8B"
    },
    "node2": {
        "base_url": "http://192.168.102.12:30001/v1",
        "api_key": "None",
        "model_path": "Qwen/Qwen3-8B"
    },
    "node3": {
        "base_url": "http://192.168.102.12:30002/v1",
        "api_key": "None",
        "model_path": "Qwen/Qwen3-8B"
    },
    "node4": {
        "base_url": "http://192.168.102.12:30003/v1",
        "api_key": "None",
        "model_path": "Qwen/Qwen3-8B"
    }
}


async def main():
    clients = {
        name: (AsyncOpenAI(base_url=info["base_url"], api_key=info["api_key"]), info["model_path"])
        for name, info in NODES_INFO.items()
    }
    await asyncio.sleep(1)

    ##### Testing code #####
    with open("results/poisson_times.json", "r", encoding="utf-8") as f:
        poisson_times = json.load(f)

    tasks = [
        asyncio.create_task(run_with_delay(item["idx"], clients, item["target"], item["problem"], item["delay"])) for item in poisson_times
    ]
    all_results = await asyncio.gather(*tasks)

    result_folder = "results/centralized_test_1/"
    os.makedirs(result_folder, exist_ok=True)

    with open(f"{result_folder}/result.json", "w", encoding="utf-8") as f:
        json.dump(
            all_results,
            f,
            ensure_ascii=False,
            indent=4,
        )


if __name__ == "__main__":
    asyncio.run(main())