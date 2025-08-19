Here’s a cleaner, README-style rewrite:

---

# Grader Branch

## Configure

Update the `base_url` field in each file under `configs/*.yaml` to match your machine (host/IP and port).

## Experiment Goal

Evaluate **credit changes** across nodes on the **MATH500** dataset.

## Topology

* **Executors (run the model):**

  * `node1`: Qwen3-14B
  * `node2`: Qwen3-8B
  * `node3`: Qwen3-8B
  * `node4`: Qwen3-8B
  * `node5`: Qwen3-4B
  * `node6`: Qwen3-0.6B
  * `node7`: Qwen3-0.6B
* **Dispatcher (no execution):**

  * `node8`: distributes requests only.

## Run

1. Launch **7** sglang servers in the background for nodes 1–7 (per your configs).
2. Execute the grader test:

   ```bash
   python tests/test_grader.py
   ```

## Output

Two CSV files recording the run will be created in `datasets/` automatically:

* `credit_timeseries_final.csv`
* `duel_stats_final.csv`

please delete them or change their name whenever you test it again.
