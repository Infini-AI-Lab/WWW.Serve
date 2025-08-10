import _setup_path
from decentralized_agents.core_node import LLMNode
from decentralized_agents.test_credit_ledger import TestCreditLedger
import asyncio
import time
import json


async def timed_submit(prompt, node: LLMNode, delay = 0):
    if delay > 0:
        await asyncio.sleep(delay)
    t0 = time.time()
    result = await node.submit_request(prompt)
    t1 = time.time()
    return result, t1 - t0, node.node_id


async def main():
    ledger = TestCreditLedger()

    # If using actual LLM servers, Set the DEBUG_MODE to False in model_manager.py!
    # Remember to adjust the credit settings in the config files.
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
    node5 = await LLMNode.init_with_ledger(
        node_id="node5",
        config_path="configs/sglang_node5.yaml",
        ledger=ledger,
    )

    await asyncio.sleep(1)

    await node1.start()
    await node2.start()
    await node3.start()
    await node4.start()
    await node5.start()

    await node2.join_network(node1.communicator.address.to_url())
    await node3.join_network(node2.communicator.address.to_url())
    await node4.join_network(node3.communicator.address.to_url())
    await node5.join_network(node4.communicator.address.to_url())

    nodes = [node1, node2, node3, node4, node5]

    ##### Testing code #####
    with open("datasets/math500/math500.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    tasks = []

    data = data + data

    for idx, item in enumerate(data):
        if idx < 200:
            tasks.append(asyncio.create_task(
                timed_submit(item["problem"], node1, idx)
            ))
        elif idx < 400:
            tasks.append(asyncio.create_task(
                timed_submit(item["problem"], node2, idx)
            ))
        elif idx < 600:
            tasks.append(asyncio.create_task(
                timed_submit(item["problem"], node3, idx)
            ))
        elif idx < 800:
            tasks.append(asyncio.create_task(
                timed_submit(item["problem"], node4, idx)
            ))
        else:
            tasks.append(asyncio.create_task(
                timed_submit(item["problem"], node5, idx)
            ))

    start = time.time()
    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start

    node_stats = {node.node_id: {"in_count": 0, "actual_count": 0, "total_time": 0.0} for node in nodes}
    for result, latency, node_name in results:
        node_stats[node_name]["in_count"] += 1
        node_stats[node_name]["total_time"] += latency
        node_stats[result["done_by"]]["actual_count"] += 1

    print(f"All prompts processed in {elapsed:.2f} seconds")
    for node_name, stats in node_stats.items():
        print(f"Node {node_name}: input {stats['in_count']} requests, actual {stats['actual_count']}, total time {stats['total_time']:.2f} seconds")

    print({node_id: account.credit for node_id, account in ledger.accounts.items()})


    with open("datasets/test_4_result.json", "w", encoding="utf-8") as f:
        json.dump(
            [
                {
                    "data": data[idx],
                    "time_taken": time_taken,
                    "result": result,
                }
                for idx, (result, time_taken, _) in enumerate(results)
            ],
            f,
            ensure_ascii=False,
            indent=2,
        )

    for idx, node in enumerate(nodes):
        with open(f"datasets/test_4_node_{idx+1}.json", "w", encoding="utf-8") as f:
            json.dump(
                node.models.server_stats_history,
                f,
                ensure_ascii=False,
                indent=2,
            )

if __name__ == "__main__":
    asyncio.run(main())