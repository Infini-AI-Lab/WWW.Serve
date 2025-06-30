import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from decentralized_agents.core_node import LLMNode
import asyncio
import time
import random
import json

cnt = [0, 0]

async def submit_prompts_batch(batch_id, prompts, node):
    start_time = time.time()

    print(f"[Batch {batch_id}] Submitting {len(prompts)} prompts to {node.node_id}...")

    tasks = [asyncio.create_task(node.submit_request(p)) for p in prompts]
    results = await asyncio.gather(*tasks)
    
    end_time = time.time()
    print(f"[Batch {batch_id}] Completed in {end_time - start_time:.2f} seconds")

    for i, r in enumerate(results):
        if r['done_by'] == "node1":
            cnt[0] += 1
        elif r['done_by'] == "node2":
            cnt[1] += 1


async def user_simulation_loop(all_prompts, nodes, sleep_range=(0.2, 1.0), batch_size_range=(1, 5)):
    current_idx = 0
    total_prompts = len(all_prompts)
    batch_id = 0
    running_batches = []

    while current_idx < total_prompts:
        await asyncio.sleep(random.uniform(*sleep_range))

        batch_id += 1
        num_prompts = random.randint(*batch_size_range)
        selected_prompts = all_prompts[current_idx:current_idx + num_prompts]
        current_idx += num_prompts

        node = random.choice(nodes)

        task = asyncio.create_task(submit_prompts_batch(batch_id, selected_prompts, node))
        running_batches.append(task)
    
    await asyncio.gather(*running_batches)

async def main():
    node1 = LLMNode(
        node_id="node1",
        ip="127.0.0.1",
        port=5678,
        config_path="../node_configs/node1.yaml",
    )
    node2 = LLMNode(
        node_id="node2",
        ip="127.0.0.1",
        port=5679,
        config_path="../node_configs/node2.yaml",
    )
    await node1.start() # 3
    await node2.start() # 3
    await node2.join_network(node1.address.to_url())


    with open("../test_datasets/test_prompts.json", "r", encoding="utf-8") as f:
        data = json.load(f)
        all_prompts = [item["prompt"] for item in data if "prompt" in item]


    start_time = time.time()
    await user_simulation_loop(all_prompts,
                               [node1, node2],
                               sleep_range=(10, 3),
                               batch_size_range=(1, 5))
    end_time = time.time()
    print(f"\nTotal simulation time: {end_time - start_time:.2f} seconds")

    print(f"Node 1 handled {cnt[0]} requests, Node 2 handled {cnt[1]} requests.")


if __name__ == "__main__":
    asyncio.run(main())

