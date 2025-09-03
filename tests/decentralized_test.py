import _setup_path
from decentralized_agents.core_node import LLMNode
from decentralized_agents.test_credit_ledger import TestCreditLedger
import asyncio
import json
import os
import time


async def timed_submit(idx, problem, node: LLMNode, delay = 0):
    if delay > 0:
        await asyncio.sleep(delay)
    submit_time = time.time()
    result = await node.submit_request(problem)
    finish_time = time.time()
    return idx, result, [submit_time, finish_time]


async def node_offline(node: LLMNode, delay=0):
    if delay > 0:
        await asyncio.sleep(delay)

    await node.stop()


async def node_start_join(node: LLMNode, url: str, delay=0):
    if delay > 0:
        await asyncio.sleep(delay)

    await node.start()
    await asyncio.sleep(1)
    await node.join_network(url)


async def main():
    ledger = TestCreditLedger()

    # node0 = await LLMNode.init_with_ledger(
    #     node_id="node0",
    #     config_path="configs/sglang_node0.yaml",
    #     ledger=ledger,
    # )
    node1 = await LLMNode.init_with_ledger(
        node_id="node1",
        config_path="configs/sglang_node1.yaml",
        ledger=ledger,
    )
    node2 = await LLMNode.init_with_ledger(
        node_id="node2",
        config_path="configs/sglang_node2.yaml",
        ledger=ledger,
    )
    node3 = await LLMNode.init_with_ledger(
        node_id="node3",
        config_path="configs/sglang_node3.yaml",
        ledger=ledger,
    )
    node4 = await LLMNode.init_with_ledger(
        node_id="node4",
        config_path="configs/sglang_node4.yaml",
        ledger=ledger,
    )

    await asyncio.sleep(1)

    # await node0.start()
    await node1.start()
    await node2.start()
    await node3.start()
    await node4.start()

    # await node1.join_network(node0.communicator.address.to_url())
    await node2.join_network(node1.communicator.address.to_url())
    await node3.join_network(node2.communicator.address.to_url())
    await node4.join_network(node3.communicator.address.to_url())

    nodes = {
        # "node0": node0,
        "node1": node1,
        "node2": node2,
        "node3": node3,
        "node4": node4
    }

    ##### Testing code #####
    with open("results/poisson_times_6.json", "r", encoding="utf-8") as f:
        poisson_times = json.load(f)

    tasks = [
        asyncio.create_task(timed_submit(item["idx"], item["problem"], nodes[item["target"]], delay=item["delay"])) for item in poisson_times
    ]

    # asyncio.create_task(node_start_join(node3, node1.communicator.address.to_url(), delay=400))
    # asyncio.create_task(node_start_join(node4, node1.communicator.address.to_url(), delay=800))
    # asyncio.create_task(node_offline(node4, delay=400))
    # asyncio.create_task(node_offline(node3, delay=800))

    all_results = await asyncio.gather(*tasks)

    result_folder = "results/decentralized_test_20/"
    os.makedirs(result_folder, exist_ok=True)

    with open(f"{result_folder}/result.json", "w", encoding="utf-8") as f:
        json.dump(
            [{
                "idx": idx,
                "timestamp_list": timestamp_list,
                **result
            } for (idx, result, timestamp_list) in all_results],
            f,
            ensure_ascii=False,
            indent=4,
        )

    for name, node in nodes.items():
        with open(f"{result_folder}/{name}.json", "w", encoding="utf-8") as f:
            json.dump(
                node.models.server_stats_history,
                f,
                ensure_ascii=False,
                indent=4,
            )

    for node in nodes.values():
        print(f"Printing {node.node_id}")
        print("pending_futures: ", node.pending_futures)
        print("delegate_from: ", node.delegate_from)
        print("send_to: ", node.send_to)
        print("dispatching_requests: ", node.dispatching_requests)
        print("Number of tasks: ", len(node._tasks))
        if len(node._tasks) != 3:
            print("Tasks: ", node._tasks)


if __name__ == "__main__":
    asyncio.run(main())