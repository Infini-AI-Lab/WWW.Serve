import _setup_path
from decentralized_agents.core_node import LLMNode
import asyncio
import time
import json



async def timed_submit(prompt, node: LLMNode):
    t0 = time.time()
    result = await node.submit_request(prompt)
    t1 = time.time()
    return result, t1 - t0


async def simulate_node_crash(node: LLMNode, delay):
    await asyncio.sleep(delay)
    await node.stop()


async def main():
    node1 = LLMNode(
        node_id="node1",
        config_path="configs/example_node1.yaml",
    )
    node2 = LLMNode(
        node_id="node2",
        config_path="configs/example_node2.yaml",
    )

    await node1.start()
    await node2.start()

    await node2.join_network(node1.communicator.address.to_url())


    with open("datasets/math500/math500.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    # data = data[:20]
    # asyncio.create_task(simulate_node_crash(node2, delay=5.0))

    start = time.time()
    tasks = [asyncio.create_task(timed_submit(item["problem"], node1)) for item in data]
    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start
    print(f"All prompts processed in {elapsed:.2f} seconds")


    with open("datasets/test_results.json", "w", encoding="utf-8") as f:
        json.dump(
            [
                {
                    "data": data[idx],
                    "time_taken": time_taken,
                    "result": result,
                }
                for idx, (result, time_taken) in enumerate(results)
            ],
            f,
            ensure_ascii=False,
            indent=4,
        )


if __name__ == "__main__":
    asyncio.run(main())