import _setup_path
from decentralized_agents.core_node import LLMNode
from decentralized_agents.test_credit_ledger import TestCreditLedger
import asyncio
import json
import random
import numpy as np
from datasets import load_dataset
import csv


def generate_text(input_token_length: int):
    if input_token_length <= 5:
        return ""
    return "PAD" * (input_token_length - 5)


async def timed_submit(prompt, node: LLMNode, delay=0, generate_token_length=None):
    if delay > 0:
        await asyncio.sleep(delay)
    result = await node.submit_request(prompt, generate_token_length=generate_token_length)
    return result


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


def poisson_time_list(rate, start_time, end_time):
    times = []
    t = start_time
    while t < end_time:
        interval = np.random.exponential(1 / rate)
        t += interval
        if t < end_time:
            times.append(t)

    return times


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
    # await asyncio.sleep(random.uniform(1, 3))
    await node2.start()
    # await asyncio.sleep(random.uniform(1, 3))
    await node3.start()
    # await asyncio.sleep(random.uniform(1, 3))
    await node4.start()
    # await asyncio.sleep(random.uniform(1, 3))
    await node5.start()
    # await asyncio.sleep(random.uniform(1, 3))

    await node2.join_network(node1.communicator.address.to_url())
    # await asyncio.sleep(random.uniform(1, 3))
    await node3.join_network(node2.communicator.address.to_url())
    # await asyncio.sleep(random.uniform(1, 3))
    await node4.join_network(node3.communicator.address.to_url())
    # await asyncio.sleep(random.uniform(1, 3))
    await node5.join_network(node4.communicator.address.to_url())
    # await asyncio.sleep(random.uniform(1, 3))

    ##### Testing code #####
    nodes = [node1, node2, node3, node4, node5]

    with open("datasets/math500/math500.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    tasks = []

    data = data[:20]

    for idx, item in enumerate(data):
        node = nodes[idx % len(nodes)]
        tasks.append(asyncio.create_task(timed_submit(item["problem"], node5, delay=idx*2)))



    all_results = await asyncio.gather(*tasks)

    print({node_id: (account.credit, account.staked) for node_id, account in ledger.accounts.items()})

    with open("datasets/test_azure_result.json", "w", encoding="utf-8") as f:
        json.dump(
            all_results,
            f,
            ensure_ascii=False,
            indent=2,
        )

    for idx, node in enumerate(nodes):
        with open(f"datasets/test_azure_node_{idx+1}.json", "w", encoding="utf-8") as f:
            json.dump(
                node.models.server_stats_history,
                f,
                ensure_ascii=False,
                indent=2,
            )


if __name__ == "__main__":
    asyncio.run(main())