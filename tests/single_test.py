import _setup_path
import asyncio
from openai import AsyncOpenAI
import json
import numpy as np
import os
import time


def poisson_time_list(rate, start_time, end_time):
    times = []
    t = start_time
    while t < end_time:
        interval = np.random.exponential(1 / rate)
        t += interval
        if t < end_time:
            times.append(t)

    return times


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
            temperature = 0.6,
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
            "timestamp_list": [submit_time, -1],
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
    return await inference_request(idx, client_name, client, model_path, problem)


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
        asyncio.create_task(submit_with_delay(item["idx"], item["target"], clients[item["target"]][0], clients[item["target"]][1], item["problem"], item["delay"]))
        for item in poisson_times
    ]
    all_results = await asyncio.gather(*tasks)

    result_folder = "results/single_test_1/"
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