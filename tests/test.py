import _setup_path
from decentralized_agents.core_node import LLMNode
from decentralized_agents.test_credit_ledger import TestCreditLedger
import asyncio
import json
import random
import numpy as np


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
    await asyncio.sleep(random.uniform(1, 3))
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
    # ledger = None

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
    await node2.start()
    await node3.start()
    await node4.start()

    await node2.join_network(node1.communicator.address.to_url())
    await node3.join_network(node2.communicator.address.to_url())
    await node4.join_network(node3.communicator.address.to_url())

    ##### Testing code #####
    with open("datasets/open-r1--OpenR1-Math-220k.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    nodes = [node1, node2, node3, node4]

    node1_times = poisson_time_list(rate=1/10, start_time=0, end_time=300)
    # asyncio.create_task(node_start_join(node2, node1.communicator.address.to_url(), delay=300))
    # asyncio.create_task(node_start_join(node3, node1.communicator.address.to_url(), delay=450))
    # asyncio.create_task(node_start_join(node4, node1.communicator.address.to_url(), delay=600))
    # asyncio.create_task(node_offline(node4, delay=300))
    # asyncio.create_task(node_offline(node3, delay=450))
    # asyncio.create_task(node_offline(node2, delay=600))

    tasks = [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node1, delay=node1_times[i])) for i in range(len(node1_times))] \
            # + [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node3, delay=node3_times[i])) for i in range(len(node3_times))] \
    #          + [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node2, delay=node2_times[i])) for i in range(len(node2_times))] \
    #          + [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node3, delay=node3_times[i])) for i in range(len(node3_times))] \
            #  + [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node4, delay=node4_times[i])) for i in range(len(node4_times))]

    all_results = await asyncio.gather(*tasks)

    with open("results/test_36_result.json", "w", encoding="utf-8") as f:
        json.dump(
            all_results,
            f,
            ensure_ascii=False,
            indent=2,
        )

    for idx, node in enumerate(nodes):
        with open(f"results/test_36_node_{idx+1}.json", "w", encoding="utf-8") as f:
            json.dump(
                node.models.server_stats_history,
                f,
                ensure_ascii=False,
                indent=2,
            )

    print(node1_times)
    for node in nodes:
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