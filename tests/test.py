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
    # node1 = await LLMNode.init_with_ledger(
    #     node_id="node1",
    #     config_path="configs/sglang_node1.yaml",
    #     ledger=ledger,
    # )
    # node2 = await LLMNode.init_with_ledger(
    #     node_id="node2",
    #     config_path="configs/sglang_node2.yaml",
    #     ledger=ledger,
    # )
    node3 = await LLMNode.init_with_ledger(
        node_id="node3",
        config_path="configs/sglang_node3.yaml",
        ledger=ledger,
    )
    # node4 = await LLMNode.init_with_ledger(
    #     node_id="node4",
    #     config_path="configs/sglang_node4.yaml",
    #     ledger=ledger,
    # )

    await asyncio.sleep(1)

    # await node1.start()
    # await asyncio.sleep(random.uniform(1, 3))
    # await node2.start()
    # await asyncio.sleep(random.uniform(1, 3))
    await node3.start()
    await asyncio.sleep(random.uniform(1, 3))
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

    # nodes = [node1, node2, node3]
    nodes = [node3]

    # node1_times = poisson_time_list(rate=0.02, start_time=0, end_time=600) \
    #               + poisson_time_list(rate=0.2, start_time=600, end_time=900) \
    #               + poisson_time_list(rate=0.02, start_time=900, end_time=3000)

    # node2_times = poisson_time_list(rate=0.01, start_time=0, end_time=3000) \
                #   + poisson_time_list(rate=1.0, start_time=180, end_time=240) \
                #   + poisson_time_list(rate=0.1, start_time=240, end_time=600)

    node3_times = [3.594811241829597, 8.951844003085466, 11.876586619317655, 14.073518107567988, 19.456148800520396, 24.524065552602053, 26.93562855261913, 29.02340050804697, 30.428648480482508, 32.63239681541187, 33.46607995767807, 44.81100557799922, 55.95419603332567, 58.72933010770205, 58.910933175824184, 61.13767055605548, 69.05972258584748, 70.76262748191249, 74.22636907774766, 77.98773030208963, 80.94699440270216, 82.79760038493043, 85.25832894782013, 89.09689501098538, 90.03926463684452, 106.2315888498608, 117.69830608063788, 119.78348623900405, 120.59135213965098, 127.43993808033437, 128.9263582885842, 133.40849760380473, 134.0687314084664, 137.53402743296522, 139.5820500720371, 145.9214618213337, 146.6349019361789, 154.5235614405197, 154.69700271131825, 154.81222119420886, 156.2172264761575, 158.71195397268255, 160.22312544975298, 164.43978985782485, 164.91064372257475, 168.83640883871422, 169.1780555448103, 173.229374782281, 173.24305491817967, 189.96591350133784, 193.87901965792713, 203.69882061796682, 204.22047682458916, 204.82347785754564, 205.05334796203059, 208.85786850782642, 210.7273641048236, 225.6777295049802, 230.2282766611644, 232.9482413380998, 235.17677714993928, 241.9736444106731, 242.50944565449777, 243.11266317964657, 244.07064925637232, 256.11644152597256, 258.98682166557745, 267.14382836374335, 274.69318253581787, 278.0644522543901, 278.5652598607103, 280.12359412903663, 285.71102264725715, 286.5191635515704, 287.3150708410264, 291.5068737344626, 294.43343553076863, 295.38541368395545, 299.2546627690117, 300.77694773877937, 301.19060518509735, 308.8277087106775, 311.2088773173099, 312.80474625157575, 314.040536025118, 316.5185607740984, 318.2599580702717, 327.220796773968, 327.2679583402943, 328.8908978588904, 330.31150999355003, 331.392173133751, 331.60925208489635, 335.72486206160795, 338.29549856835814, 344.1407688801069, 353.9577432795879, 355.2331565286666, 359.2882424285891, 363.3842744163178, 370.3630624480978, 370.72621431464194, 371.84038379222204, 377.0251942297398, 384.3004630669902, 387.505849276289, 389.5837290266366, 391.78793301310765, 392.4824955415095, 392.95905118493647, 394.4820586936255, 395.23888936104515, 403.6139176009672, 404.8531721694696, 407.29774268042655, 410.7321582535817, 427.4299778278163, 427.65246348378537, 432.4417303791699, 434.9447737570764, 437.66162745942466, 443.0210735095181, 454.98362796248716, 461.1909392360133, 461.60572051400777, 469.3122190941054, 469.6400763074821, 475.5446145984178, 475.68260515493694, 478.3225023023486, 479.1094493518623, 481.10068428937353, 481.2936955557075, 489.8990657223163, 491.06000354182794, 493.27747904204284, 495.9379851387453, 501.6399381846576, 512.3073271995112, 514.0111433292509, 514.853867092085, 516.6463117664579, 518.4886563892472, 519.1068808765685, 520.7027190760967, 523.8496042200093, 525.7017451596877, 526.0569630840911, 530.2372165975498, 531.4924903531121, 537.8190229421518, 538.3726099240188, 543.7092589886921, 544.8787910215319, 546.5513457984734, 549.6707523154444, 550.6441156527412, 559.557857701809, 571.0905251988816, 571.8086720070966, 580.2950083713341, 582.6833863958735, 589.2225373497322, 589.6052188108033, 591.1940921846616, 599.953798681215, 621.846699952718, 660.374184779799, 666.6312906317722, 687.9141694773716, 697.1125255084311, 763.9554253381243, 939.9983730268035, 1077.1767006897962, 1089.4320192675405, 1126.8956339650833, 1130.8219227245552, 1156.7242496458275, 1200.386636461175, 1234.8839905812558, 1238.9151644359736, 1379.4409913253864, 1409.1564949988974, 1457.806028152763, 1460.2028105640522, 1516.4015125850171, 1578.4934264347196, 1642.6014589873114, 1655.073744238124, 1718.7768386099447, 1741.4673854893665, 1771.587930529683, 1774.4292129003481, 1777.988108377415]

    # node3_times = poisson_time_list(rate=0.3, start_time=0, end_time=600) \
    #               + poisson_time_list(rate=0.03, start_time=600, end_time=1800) \
                #   + poisson_time_list(rate=0.02, start_time=1200, end_time=3000)

    # node4_times = poisson_time_list(rate=0.025, start_time=0, end_time=3000) \
                #   + poisson_time_list(rate=0.5, start_time=480, end_time=600) \
                #   + poisson_time_list(rate=0.1, start_time=600, end_time=900)

    # tasks = [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node1, delay=node1_times[i])) for i in range(len(node1_times))] \
    #          + [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node2, delay=node2_times[i])) for i in range(len(node2_times))] \
    #          + [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node3, delay=node3_times[i])) for i in range(len(node3_times))] \
            #  + [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node4, delay=node4_times[i])) for i in range(len(node4_times))]

    tasks = [asyncio.create_task(timed_submit(data[i % len(data)]["problem"], node3, delay=node3_times[i])) for i in range(len(node3_times))]

    all_results = await asyncio.gather(*tasks)

    with open("results/test_8_result.json", "w", encoding="utf-8") as f:
        json.dump(
            all_results,
            f,
            ensure_ascii=False,
            indent=2,
        )

    for idx, node in enumerate(nodes):
        with open(f"results/test_8_node_{idx+1}.json", "w", encoding="utf-8") as f:
            json.dump(
                node.models.server_stats_history,
                f,
                ensure_ascii=False,
                indent=2,
            )

    print(node3_times)


if __name__ == "__main__":
    asyncio.run(main())