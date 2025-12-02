import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import asyncio
import json
import time
from pathlib import Path

import random
random.seed(42)


from www_serve.core_node import LLMNode
from www_serve.credit_ledger import CreditLedger


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


async def check_final_stats(node: LLMNode):
    print(f"Printing {node.node_id}")
    print(f"Credit Account: ", await node.credit_ledger.get_account_credit(node.node_id))
    print("pending_futures: ", await node.pending_futures.items())
    print("delegate_from: ", await node.delegate_from.items())
    print("send_to: ", await node.send_to.items())
    print("dispatching_requests: ", await node.dispatching_requests.items())
    print("duel_settle_locks: ", await node.duel_settle_locks.items())
    print("duel_states: ", await node.duel_states.items())
    print("duel_dict: ", await node.duel_dict.items())
    print("judge_dict: ", await node.judge_dict.items())
    print("Number of tasks: ", len(node._tasks))
    if len(node._tasks) != 3:
        print("Tasks: ", node._tasks)



async def main():

    ##### Initialize nodes and credit ledger #####
    # TODO: Change to User input path
    RESULT_PATH = Path(__file__).parent.parent / "results"
    result_folder = RESULT_PATH / f"decentralized_simulation"
    os.makedirs(result_folder, exist_ok=True)
    ledger = CreditLedger(duel_record_pth=result_folder / "duel_record.csv")

    CONFIG_PATH = Path(__file__).parent.parent.parent / "node_configs"

    node0 = await LLMNode.init_with_ledger(
        node_id="node0",
        config_path=CONFIG_PATH / "node0.yaml",
        ledger=ledger,
    )
    node1 = await LLMNode.init_with_ledger(
        node_id="node1",
        config_path=CONFIG_PATH / "node1.yaml",
        ledger=ledger,
    )
    node2 = await LLMNode.init_with_ledger(
        node_id="node2",
        config_path=CONFIG_PATH / "node2.yaml",
        ledger=ledger,
    )
    node3 = await LLMNode.init_with_ledger(
        node_id="node3",
        config_path=CONFIG_PATH / "node3.yaml",
        ledger=ledger,
    )
    node4 = await LLMNode.init_with_ledger(
        node_id="node4",
        config_path=CONFIG_PATH / "node4.yaml",
        ledger=ledger,
    )

    await asyncio.sleep(1)

    ##### Start nodes and form network #####
    await node0.start()
    await node1.start()
    await node2.start()
    await node3.start()
    await node4.start()

    await node1.join_network(node0.communicator.address.to_url())
    await node2.join_network(node1.communicator.address.to_url())
    await node3.join_network(node2.communicator.address.to_url())
    await node4.join_network(node3.communicator.address.to_url())

    nodes = {
        "node0": node0,
        "node1": node1,
        "node2": node2,
        "node3": node3,
        "node4": node4,
    }

    ##### Load input requests and simulate #####
    input_path = Path(__file__).parent / "simu_input.json"
    with open(input_path, "r", encoding="utf-8") as f:
        poisson_times = json.load(f)

    tasks = [
        asyncio.create_task(timed_submit(item["idx"], item["problem"], nodes[item["target"]], delay=item["delay"])) for item in poisson_times
    ]

    ##### Simulate node dynamics (uncomment to enable) #####
    # asyncio.create_task(node_start_join(node4, node1.communicator.address.to_url(), delay=800))
    # asyncio.create_task(node_offline(node4, delay=400))

    all_results = await asyncio.gather(*tasks)

    # TODO: For now, wait until all duel requests are done
    current_time = time.time()
    while time.time() - current_time < 300:
        finished = True
        for node in nodes.values():
            if len(node._tasks) != 3:
                finished = False

        if finished:
            break
        await asyncio.sleep(5)


    ##### Save results and stats #####
    RESULT_PATH = Path(__file__).parent.parent / "results"
    result_folder = RESULT_PATH / f"decentralized_simulation"
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
        await check_final_stats(node)




if __name__ == "__main__":
    asyncio.run(main())