import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import asyncio
import json
import time
from pathlib import Path
import random

from www_serve.core_node import LLMNode
from www_serve.credit_ledger import CreditLedger


async def init_all_nodes(num_nodes, config_path, ledger) -> dict[str, LLMNode]:
    nodes = {}
    for i in range(num_nodes):
        node = await LLMNode.init_with_ledger(
            node_id=f"node{i}",
            config_path=config_path / f"node{i}.yaml",
            ledger=ledger,
        )
        nodes[f"node{i}"] = node
    return nodes


async def start_all_nodes(nodes: dict[str, LLMNode]):
    for node in nodes.values():
        delay = random.uniform(0.5, 2.5)
        await asyncio.sleep(delay)
        await node.start()


async def construct_network(nodes: dict[str, LLMNode]):
    node_list = list(nodes.values())
    for i in range(1, len(node_list)):
        delay = random.uniform(0.5, 2.5)
        await asyncio.sleep(delay)
        await node_list[i].join_network(node_list[i-1].communicator.address.to_url())


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


async def save_simu_results(nodes: dict[str, LLMNode], all_results, result_folder: Path):
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


async def check_final_stats(nodes: dict[str, LLMNode]):
    for node in nodes.values():
        print(f"Printing {node.node_id}")
        print(f"Credit Account: ", await node.credit_ledger.get_account_credit(node.node_id))
        print("Number of tasks: ", len(node._tasks))
        if len(node._tasks) != 4:
            print("Tasks: ", node._tasks)
            print("pending_futures: ", await node.pending_futures.items())
            print("delegate_from: ", await node.delegate_from.items())
            print("send_to: ", await node.send_to.items())
            print("dispatching_requests: ", await node.dispatching_requests.items())
            print("duel_settle_locks: ", await node.duel_settle_locks.items())
            print("duel_states: ", await node.duel_states.items())
            print("duel_dict: ", await node.duel_dict.items())
            print("judge_dict: ", await node.judge_dict.items())


async def main():
    ##### Initialize nodes and credit ledger #####
    RESULT_PATH = Path(__file__).parent.parent / "results"
    CONFIG_PATH = Path(__file__).parent.parent.parent / "node_configs"

    result_folder = RESULT_PATH / f"decentralized_simulation"
    if result_folder.exists():
        print(f"Result folder {result_folder} already exists. Please move or delete it before running the simulation.")
        return

    os.makedirs(result_folder, exist_ok=True)
    ledger = CreditLedger(duel_record_pth=result_folder / "duel_record.csv")

    nodes = await init_all_nodes(7, CONFIG_PATH, ledger)
    await start_all_nodes(nodes)
    await construct_network(nodes)

    ##### Load input requests and simulate #####
    input_path = Path(__file__).parent / "simu_input.json"
    # input_path = Path(__file__).parent / "math500.json"

    with open(input_path, "r", encoding="utf-8") as f:
        poisson_times = json.load(f)

    tasks = [
        asyncio.create_task(timed_submit(item["idx"], item["problem"], nodes[item["target"]], delay=item["delay"])) for item in poisson_times
    ]

    # tasks = [
    #     asyncio.create_task(timed_submit(idx, item["problem"], nodes["node0"], delay=idx)) for idx, item in enumerate(poisson_times)
    # ]

    ##### Simulate node dynamics (uncomment to enable) #####
    # asyncio.create_task(node_start_join(node4, node1.communicator.address.to_url(), delay=800))
    # asyncio.create_task(node_offline(node4, delay=400))

    all_results = await asyncio.gather(*tasks)

    # TODO: For now, wait until all duel requests are done
    current_time = time.time()
    while time.time() - current_time < 300:
        finished = True
        for node in nodes.values():
            if len(node._tasks) != 4:
                finished = False

        if finished:
            break
        await asyncio.sleep(5)


    ##### Save results and stats #####
    await save_simu_results(nodes, all_results, result_folder)
    await check_final_stats(nodes)




if __name__ == "__main__":
    asyncio.run(main())