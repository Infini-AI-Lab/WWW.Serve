import csv
from modulefinder import test
import _setup_path
from decentralized_agents.core_node import LLMNode
from decentralized_agents.test_credit_ledger import TestCreditLedger
import asyncio
import json
import random
import numpy as np
from datasets import load_dataset
import time
import os

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

# read the account credits and stakes every 10s
async def account_sampler(ledger: TestCreditLedger, stop_evt: asyncio.Event, credit_stake_csv_path, interval: float = 10.0):
    while not stop_evt.is_set():
        ts = time.time()
        async with ledger.accounts_lock.reader_lock:
            items = list(ledger.accounts.items())
        with open(credit_stake_csv_path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for node_id, account in items:
                w.writerow([ts, node_id, account.credit, account.staked])
        await asyncio.sleep(interval)


async def main():
    ledger = TestCreditLedger(duel_record_pth="datasets/duel_stats.csv")
    credit_stake_csv_path = "datasets/credit_timeseries.csv"

    # If using actual LLM servers, Set the DEBUG_MODE to False in model_manager.py!
    # Remember to adjust the credit settings in the config files.
    node1 = await LLMNode.init_with_ledger(
        node_id="node1",
        config_path="configs/sglang_node1.yaml",
        ledger=ledger,
        test=False,
    )
    node2 = await LLMNode.init_with_ledger(
        node_id="node2",
        config_path="configs/sglang_node2.yaml",
        ledger=ledger,
        test=False,
    )
    node3 = await LLMNode.init_with_ledger(
        node_id="node3",
        config_path="configs/sglang_node3.yaml",
        ledger=ledger,
        test=False,
    )
    node4 = await LLMNode.init_with_ledger(
        node_id="node4",
        config_path="configs/sglang_node4.yaml",
        ledger=ledger,
        test=True,
    )
    node5 = await LLMNode.init_with_ledger(
        node_id="node5",
        config_path="configs/sglang_node5.yaml",
        ledger=ledger,
        test=True,
    )
    # node6 = await LLMNode.init_with_ledger(
    #     node_id="node6",
    #     config_path="configs/sglang_node6.yaml",
    #     ledger=ledger,
    #     test=True,
    # )
    node7 = await LLMNode.init_with_ledger(
        node_id="node7",
        config_path="configs/sglang_node7.yaml",
        ledger=ledger,
        test=True,
    )
    # node8 = await LLMNode.init_with_ledger(
    #     node_id="node8",
    #     config_path="configs/sglang_node8.yaml",
    #     ledger=ledger,
    #     test=True,
    # )
    


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
    # await node6.start()
    
    await node7.start()

    # await node8.start()

    await node2.join_network(node1.communicator.address.to_url())
    await asyncio.sleep(random.uniform(1, 3))
    await node3.join_network(node2.communicator.address.to_url())
    await asyncio.sleep(random.uniform(1, 3))
    await node4.join_network(node3.communicator.address.to_url())
    await asyncio.sleep(random.uniform(1, 3))
    await node5.join_network(node4.communicator.address.to_url())
    await asyncio.sleep(random.uniform(1, 3))
    # await node6.join_network(node5.communicator.address.to_url())

    await node7.join_network(node4.communicator.address.to_url())

    # await node8.join_network(node7.communicator.address.to_url())

    ##### Testing code #####
    nodes = [node1, node2, node3, node4, node5, node7]

    with open("datasets/math500/math500.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    tasks = []

    data = data + data + data + data
    #submit 4 tasks every 10 seconds
    
    for idx, item in enumerate(data):
        delay = (idx // 4) * 9
        delay += idx % 4
        print("submit", idx, "after", delay, "seconds")
        tasks.append(asyncio.create_task(timed_submit(item["problem"], node7, delay=delay)))


    # create a csv file if it doesn't exist
    os.makedirs(os.path.dirname(credit_stake_csv_path), exist_ok=True)
    
    stop_evt = asyncio.Event()
    asyncio.create_task(account_sampler(ledger, stop_evt, credit_stake_csv_path, interval=10.0))

    await asyncio.sleep(60*90)  # 运行 90 分钟
    stop_evt.set()


    all_results = await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())