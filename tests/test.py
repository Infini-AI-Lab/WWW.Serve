import _setup_path
from decentralized_agents.core_node import LLMNode
from decentralized_agents.test_credit_ledger import TestCreditLedger
import asyncio
import time
import json


async def timed_submit(prompt, node: LLMNode):
    t0 = time.time()
    result = await node.submit_request(prompt)
    t1 = time.time()
    return result, t1 - t0


async def simulate_node_crash(node: LLMNode, delay):
    await asyncio.sleep(delay)
    await node.stop()


async def main():
    ledger = TestCreditLedger()

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
    # node4 = await LLMNode.init(
    #     node_id="node4",
    #     config_path="configs/vllm_node1.yaml",
    # )

    await asyncio.sleep(1)

    await node1.start()
    await node2.start()
    await node3.start()
    # await node4.start()

    await node2.join_network(node1.communicator.address.to_url())
    await node3.join_network(node2.communicator.address.to_url())
    # await node4.join_network(node3.communicator.address.to_url())

    # await node4.join_network(node3.communicator.address.to_url())
    # await asyncio.sleep(2)


    with open("datasets/math500/math500.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    data = data[:5]
    # data = data + data + data

    # asyncio.create_task(simulate_node_crash(node2, delay=5.0))

    start = time.time()
    tasks = [asyncio.create_task(timed_submit(item["problem"], node1)) for item in data]
    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start
    print(f"All prompts processed in {elapsed:.2f} seconds")

    print(ledger.accounts)
    print(ledger.stakes)


    # with open("datasets/test_results_s_7B_7B_1.5B_2.json", "w", encoding="utf-8") as f:
    #     json.dump(
    #         [
    #             {
    #                 "data": data[idx],
    #                 "time_taken": time_taken,
    #                 "result": result,
    #             }
    #             for idx, (result, time_taken) in enumerate(results)
    #         ],
    #         f,
    #         ensure_ascii=False,
    #         indent=4,
    #     )


if __name__ == "__main__":
    asyncio.run(main())