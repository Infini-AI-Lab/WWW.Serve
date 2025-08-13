import _setup_path
from decentralized_agents.core_node import LLMNode
from decentralized_agents.test_credit_ledger import TestCreditLedger
import asyncio
import json
import random
import numpy as np
from datasets import load_dataset

# WHAT? Will damage zmq connection!
data = load_dataset("HuggingFaceH4/MATH-500")['test']


async def timed_submit(prompt, node: LLMNode, delay = 0):
    if delay > 0:
        await asyncio.sleep(delay)
    result = await node.submit_request(prompt)
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

    await asyncio.sleep(1)

    await node1.start()
    await asyncio.sleep(random.uniform(1, 3))
    await node2.start()
    await asyncio.sleep(random.uniform(1, 3))
    await node3.start()
    await asyncio.sleep(random.uniform(1, 3))
    await node4.start()
    await asyncio.sleep(random.uniform(1, 3))

    await node2.join_network(node1.communicator.address.to_url())
    await asyncio.sleep(random.uniform(1, 3))
    await node3.join_network(node2.communicator.address.to_url())
    await asyncio.sleep(random.uniform(1, 3))
    await node4.join_network(node3.communicator.address.to_url())
    await asyncio.sleep(random.uniform(1, 3))

    ##### Testing code #####
    nodes = [node1, node2, node3, node4]

    node1_times = poisson_time_list(rate=0.2, start_time=0, end_time=60) \
                  + poisson_time_list(rate=2.0, start_time=60, end_time=120) \
                  + poisson_time_list(rate=0.2, start_time=120, end_time=600)

    node2_times = poisson_time_list(rate=0.2, start_time=0, end_time=180) \
                  + poisson_time_list(rate=2.0, start_time=180, end_time=240) \
                  + poisson_time_list(rate=0.2, start_time=240, end_time=600)

    node3_times = poisson_time_list(rate=0.2, start_time=0, end_time=300) \
                  + poisson_time_list(rate=2.0, start_time=300, end_time=360) \
                  + poisson_time_list(rate=0.2, start_time=360, end_time=600)

    node4_times = poisson_time_list(rate=0.2, start_time=0, end_time=420) \
                  + poisson_time_list(rate=2.0, start_time=420, end_time=480) \
                  + poisson_time_list(rate=0.2, start_time=480, end_time=600)

    tasks = [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node1, delay=node1_times[i])) for i in range(len(node1_times))] \
             + [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node2, delay=node2_times[i])) for i in range(len(node2_times))] \
             + [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node3, delay=node3_times[i])) for i in range(len(node3_times))] \
             + [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node4, delay=node4_times[i])) for i in range(len(node4_times))]

    all_results = await asyncio.gather(*tasks)

    print({node_id: (account.credit, account.staked) for node_id, account in ledger.accounts.items()})

    with open("datasets/test_1_result.json", "w", encoding="utf-8") as f:
        json.dump(
            all_results,
            f,
            ensure_ascii=False,
            indent=2,
        )

    for idx, node in enumerate(nodes):
        with open(f"datasets/test_1_node_{idx+1}.json", "w", encoding="utf-8") as f:
            json.dump(
                node.models.server_stats_history,
                f,
                ensure_ascii=False,
                indent=2,
            )


if __name__ == "__main__":
    asyncio.run(main())