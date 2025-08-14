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
    # ledger = TestCreditLedger()
    ledger = None

    # If using actual LLM servers, Set the DEBUG_MODE to False in model_manager.py!
    # Remember to adjust the credit settings in the config files.
    node1 = await LLMNode.init_with_ledger(
        node_id="node1",
        config_path="configs/sglang_node1.yaml",
        ledger=ledger,
    )
    # node2 = await LLMNode.init_with_ledger(
    #     node_id="node2",
    #     config_path="configs/sglang_node2.yaml",
    #     ledger=ledger,
    # )
    # node3 = await LLMNode.init_with_ledger(
    #     node_id="node3",
    #     config_path="configs/sglang_node3.yaml",
    #     ledger=ledger,
    # )
    # node4 = await LLMNode.init_with_ledger(
    #     node_id="node4",
    #     config_path="configs/sglang_node4.yaml",
    #     ledger=ledger,
    # )

    await asyncio.sleep(1)

    await node1.start()
    await asyncio.sleep(random.uniform(1, 3))
    # await node2.start()
    # await asyncio.sleep(random.uniform(1, 3))
    # await node3.start()
    # await asyncio.sleep(random.uniform(1, 3))
    # await node4.start()
    # await asyncio.sleep(random.uniform(1, 3))

    # await node2.join_network(node1.communicator.address.to_url())
    # await asyncio.sleep(random.uniform(1, 3))
    # await node3.join_network(node2.communicator.address.to_url())
    # await asyncio.sleep(random.uniform(1, 3))
    # await node4.join_network(node3.communicator.address.to_url())
    # await asyncio.sleep(random.uniform(1, 3))

    ##### Testing code #####
    with open("datasets/open-r1--OpenR1-Math-220k.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    # nodes = [node1, node2, node3, node4]
    nodes = [node1]

    node1_times = [27.771473488537886, 82.66264938743856, 84.36998482005988, 102.56824631921661, 123.44327517212281, 124.28589509679476, 128.9197269092678, 333.5361535297029, 335.7994848891475, 346.27747634351294, 351.2461603485821, 361.1983477223536, 363.7146121629658, 366.41635246144716, 376.1968556077908, 378.0364371940009, 383.71539257657054, 391.4992045382991, 399.90145827212257, 407.38253548220194, 418.2698795772522, 419.143939502819, 438.64058129436324, 445.15886353352204, 459.3389939539733, 459.56273271114014, 474.4173502824392, 476.2895091469314, 477.24908267112016, 482.7572537293996, 483.83754315808824, 486.24018954191143, 489.4097702506401, 496.68651903421295, 497.88373005292874, 499.327152366629, 502.973714780783, 512.2259299647286, 515.9148082192376, 516.9148801294646, 523.6170223808962, 530.1650384758749, 532.3580151837823, 533.7162347047382, 534.612001893687, 537.4044990257005, 544.9845117782569, 550.8618126026206, 554.2622344957074, 563.3963836963131, 583.995928431227, 584.6548242968695, 585.090210336475, 585.3566440158978, 587.7493026034325, 592.4579991003692, 621.0573952250754, 623.5150934624999, 654.0767249470598, 670.3732629170555, 670.5729647455859, 692.3291946169758, 696.4708663804886, 754.466030244662, 861.0900130518106]

    # node1_times = poisson_time_list(rate=0.02, start_time=0, end_time=300) \
    #               + poisson_time_list(rate=0.2, start_time=300, end_time=600) \
    #               + poisson_time_list(rate=0.02, start_time=600, end_time=900)

    # node2_times = poisson_time_list(rate=0.05, start_time=0, end_time=1200) \
                #   + poisson_time_list(rate=1.0, start_time=180, end_time=240) \
                #   + poisson_time_list(rate=0.1, start_time=240, end_time=600)

    # node3_times = poisson_time_list(rate=0.05, start_time=0, end_time=1200) \
                #   + poisson_time_list(rate=1.0, start_time=300, end_time=360) \
                #   + poisson_time_list(rate=0.1, start_time=360, end_time=600)

    # node4_times = poisson_time_list(rate=0.05, start_time=0, end_time=1200) \
                #   + poisson_time_list(rate=0.5, start_time=480, end_time=600) \
                #   + poisson_time_list(rate=0.1, start_time=600, end_time=900)

    tasks = [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node1, delay=node1_times[i])) for i in range(len(node1_times))] \
            #  + [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node2, delay=node2_times[i])) for i in range(len(node2_times))] \
            #  + [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node3, delay=node3_times[i])) for i in range(len(node3_times))] \
            # + [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node4, delay=node4_times[i])) for i in range(len(node4_times))]

    all_results = await asyncio.gather(*tasks)

    with open("results/test_4_result.json", "w", encoding="utf-8") as f:
        json.dump(
            all_results,
            f,
            ensure_ascii=False,
            indent=2,
        )

    for idx, node in enumerate(nodes):
        with open(f"results/test_4_node_{idx+1}.json", "w", encoding="utf-8") as f:
            json.dump(
                node.models.server_stats_history,
                f,
                ensure_ascii=False,
                indent=2,
            )
    
    print(node1_times)


if __name__ == "__main__":
    asyncio.run(main())