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
import sys

# Only support single model per node for now
def _parse_node_config(config_path) -> Dict:
    with open(config_path, "r", encoding="utf-8") as f:
        node_cfg = yaml.safe_load(f)
    return {
        "base_url": node_cfg["models"][0]["base_url"],
        "api_key": node_cfg["models"][0].get("api_key", "None"),
        "model_path": node_cfg["models"][0]["model_path"],
        "policy": node_cfg["server_params"]["policy"],
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
        parsed_metrics: List[Dict] = []

        try:
            families = text_string_to_metric_families(metrics_text)
            for family in families:
                for sample in family.samples:
                    if metric_list is None or sample.name in metric_list:
                        parsed_metrics.append({
                            "name": sample.name,
                            "value": sample.value,
                            "labels": sample.labels
                        })
        except Exception as e:
            pass
        return parsed_metrics

    except Exception as e:
        return None


async def get_server_metrics(server_url: str,
                             policy: str,
                             is_sglang: bool,
                             ) -> Dict[str, float | int]:
    """
        Get server metrics for the model.
        Returns a dict of parsed metrics mapping metric name to numeric value (float or int).
    """
    match(policy):
        case "default_mlc_llm":
            parsed_metrics = {
                "prefill_tokens_per_s": 0.0,
                "last_finished_request_end_to_end_latency_s": 0.0,
                "last_finished_request_ttft_s": 0.0
            }

            queried_metrics = ["prefill_tokens_per_s",
                                                "last_finished_request_end_to_end_latency_s",
                                                "last_finished_request_ttft_s"]
            metrics = await _get_server_metrics(server_url, metric_list=queried_metrics)
            if not metrics:
                return parsed_metrics

            raw_metrics = {entry["name"]: entry["value"] for entry in metrics}

            for metric in queried_metrics:
                parsed_metrics[metric] = raw_metrics.get(metric, 0.0)
            
            return parsed_metrics
        case _: 
            parsed_metrics = {
                "running_reqs": 999,
                "queue_reqs": 999,
                "token_usage": 1.0
            }
            if is_sglang:
                queried_metrics = ["sglang:num_running_reqs",
                                    "sglang:num_queue_reqs",
                                    "sglang:token_usage"]
            else:
                queried_metrics = ["vllm:num_requests_running",
                                "vllm:num_requests_waiting",
                                "vllm:gpu_cache_usage_perc"]
            metrics = await _get_server_metrics(server_url, metric_list=queried_metrics)

            if not metrics:
                return parsed_metrics

            raw_metrics = {entry["name"]: entry["value"] for entry in metrics}

            if is_sglang:
                token_usage = raw_metrics.get("sglang:token_usage", 1.0)
                num_running_reqs = raw_metrics.get("sglang:num_running_reqs", 999)
                num_queue_reqs = raw_metrics.get("sglang:num_queue_reqs", 999)
            else:
                token_usage = raw_metrics.get("vllm:gpu_cache_usage_perc", 1.0)
                num_running_reqs = raw_metrics.get("vllm:num_requests_running", 999)
                num_queue_reqs = raw_metrics.get("vllm:num_requests_waiting", 999)

            parsed_metrics["token_usage"] = token_usage
            parsed_metrics["running_reqs"] = int(num_running_reqs)
            parsed_metrics["queue_reqs"] = int(num_queue_reqs)

            return parsed_metrics


class RecordStats:
    def __init__(self, server_url, policy, model_path, is_sglang):
        self.stats = []
        self.server_url = server_url
        self.policy = policy
        self.model_path = model_path
        self.is_sglang = is_sglang
        asyncio.create_task(self.record_stats_periodically())

    async def record_stats_periodically(self):
        while True:
            metrics = await get_server_metrics(server_url=self.server_url, policy=self.policy, is_sglang=self.is_sglang)

            record = {"timestamp": time.time()}
            for name, value in metrics.items():
                record[name] = value

            self.stats.append(record)

            print(f"{self.server_url}: metrics={metrics}")
            # print(f"{self.server_url}: running={num_running_reqs}, queue={num_queue_reqs}, token={token_usage}")

            await asyncio.sleep(3)


async def inference_request(idx, client_name, client, model_path, problem):
    submit_time = time.time()
    try:
        print(f"Dispatching request {idx} -> {client_name}")
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

        print(f"Received response for {idx} from {client_name} (took {finish_time - submit_time:.2f}s)")

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
        name: (AsyncOpenAI(base_url=f"{info["base_url"]}/v1", api_key=info["api_key"]), info["model_path"])
        for name, info in NODES_INFO.items()
    }
    await asyncio.sleep(1)

    ##### Start recording stats #####
    stats = {
        name: RecordStats(info["base_url"], policy=info.get("policy"), model_path=info.get("model_path"), is_sglang=info["is_sglang"])
        for name, info in NODES_INFO.items()
    }
    await asyncio.sleep(1)

    ##### Load input requests #####
    input_path = Path(__file__).parent / "simu_input.json"
    with open(input_path, "r", encoding="utf-8") as f:
        poisson_times = json.load(f)

    tasks = [
        asyncio.create_task(submit_with_delay(item["idx"], item["target"], clients[item["target"]][0], clients[item["target"]][1], item["problem"], item["delay"]))
        for item in poisson_times
    ]
    all_results = await asyncio.gather(*tasks)

    ##### Save results #####
    RESULT_PATH = Path(__file__).parent.parent / "results"
    result_folder = RESULT_PATH / f"single_simulation"
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