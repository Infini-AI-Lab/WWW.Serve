import numpy as np
import json

def poi(rate, start_time, end_time):
    times = []
    t = start_time
    while t < end_time:
        interval = np.random.exponential(1 / rate)
        t += interval
        if t < end_time:
            times.append(t)

    return times


# node1_times = poi(rate=1/2.5, start_time=0, end_time=450) + poi(rate=1/15, start_time=450, end_time=1200)
# node2_times = poi(rate=1/6, start_time=0, end_time=450) + poi(rate=1/25, start_time=450, end_time=1200)
# node3_times = poi(rate=1/30, start_time=0, end_time=750) + poi(rate=1/9, start_time=750, end_time=1200)
# node4_times = poi(rate=1/25, start_time=0, end_time=750) + poi(rate=1/6, start_time=750, end_time=1200)

node0_times = poi(rate=1/4, start_time=0, end_time=1200)

# print("len(node1_times):", len(node1_times))
# print("len(node2_times):", len(node2_times))
# print("len(node3_times):", len(node3_times))
# print("len(node4_times):", len(node4_times))

with open("datasets/open-r1--OpenR1-Math-220k.json", "r", encoding="utf-8") as f:
        data = json.load(f)


problems = [
     {
        "problem": data[i+3000]["problem"],
        "target": "node0",
        "delay": node0_times[i]
    } for i in range(len(node0_times))
]


# problems = [
#     {
#         "problem": data[i]["problem"],
#         "target": "node1",
#         "delay": node1_times[i]
#     } for i in range(len(node1_times))
# ] + [
#     {
#         "problem": data[i+2000]["problem"],
#         "target": "node2",
#         "delay": node2_times[i]
#     } for i in range(len(node2_times))
# ] + [
#     {
#         "problem": data[i+4000]["problem"],
#         "target": "node3",
#         "delay": node3_times[i]
#     } for i in range(len(node3_times))
# ] + [
#     {
#         "problem": data[i+6000]["problem"],
#         "target": "node4",
#         "delay": node4_times[i]
#     } for i in range(len(node4_times))
# ]

with open("results/poisson_times.json", "w", encoding="utf-8") as f:
    json.dump([{
              "idx": idx,
              **problem
         } for idx, problem in enumerate(problems)], f, ensure_ascii=False, indent=4)