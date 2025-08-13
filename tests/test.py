import _setup_path
from decentralized_agents.core_node import LLMNode
from decentralized_agents.test_credit_ledger import TestCreditLedger
import asyncio
import time
import json
import random


async def timed_submit(prompt, node: LLMNode, delay = 0):
    if delay > 0:
        await asyncio.sleep(delay)
    result = await node.submit_request(prompt)
    return result, node.node_id


async def node_offline(node: LLMNode, delay=0):
    if delay > 0:
        await asyncio.sleep(delay)

    await node.stop()


async def node_start_join(node: LLMNode, url: str, delay=0):
    if delay > 0:
        await asyncio.sleep(delay)

    await node.start()
    await asyncio.sleep(random.uniform(1, 5))

    await node.join_network(url)


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
    # node5 = await LLMNode.init_with_ledger(
    #     node_id="node5",
    #     config_path="configs/sglang_node5.yaml",
    #     ledger=ledger,
    # )

    await asyncio.sleep(1)

    await node1.start()
    await asyncio.sleep(random.uniform(1, 5))
    await node2.start()
    await asyncio.sleep(random.uniform(1, 5))
    await node3.start()
    await asyncio.sleep(random.uniform(1, 5))
    await node4.start()
    await asyncio.sleep(random.uniform(1, 5))
    # await node5.start()
    # await asyncio.sleep(random.uniform(1, 5))

    await node2.join_network(node1.communicator.address.to_url())
    await asyncio.sleep(random.uniform(1, 5))
    await node3.join_network(node2.communicator.address.to_url())
    await asyncio.sleep(random.uniform(1, 5))
    await node4.join_network(node3.communicator.address.to_url())
    await asyncio.sleep(random.uniform(1, 5))
    # await node5.join_network(node4.communicator.address.to_url())
    # await asyncio.sleep(random.uniform(1, 5))

    print({node_id: (account.credit, account.staked) for node_id, account in ledger.accounts.items()})

    ##### Testing code #####
    nodes = [node1, node2, node3, node4]

    with open("datasets/math500/math500.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    tasks = []

    # data = data + data

    for idx, item in enumerate(data):
        if idx < 150:
            tasks.append(asyncio.create_task(
                timed_submit(item["problem"], node1)
            ))
        elif idx < 300:
            tasks.append(asyncio.create_task(
                timed_submit(item["problem"], node2, 150)
            ))
        elif idx < 500:
            tasks.append(asyncio.create_task(
                timed_submit(item["problem"], node3, 300)
            ))
        # elif idx < 1000:
        #     tasks.append(asyncio.create_task(
        #         timed_submit(item["problem"], node4, 750)
        #     ))
        # else:
        #     tasks.append(asyncio.create_task(
        #         timed_submit(item["problem"], node5, idx)
        #     ))

    start = time.time()
    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start

    node_stats = {node.node_id: {"in_count": 0, "actual_count": 0, "total_time": 0.0} for node in nodes}
    for result, node_name in results:
        node_stats[node_name]["in_count"] += 1
        node_stats[node_name]["total_time"] += (result["timestamp_list"][3] - result["timestamp_list"][0])
        node_stats[result["response"]["done_by"]]["actual_count"] += 1

    print(f"All prompts processed in {elapsed:.2f} seconds")
    for node_name, stats in node_stats.items():
        print(f"Node {node_name}: input {stats['in_count']} requests, actual {stats['actual_count']}, total time {stats['total_time']:.2f} seconds")

    print({node_id: (account.credit, account.staked) for node_id, account in ledger.accounts.items()})


    with open("datasets/test_4_result.json", "w", encoding="utf-8") as f:
        json.dump(
            [
                {
                    "data": data[idx],
                    "result": result,
                }
                for idx, (result, _) in enumerate(results)
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