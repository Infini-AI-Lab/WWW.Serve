import aiohttp
import asyncio
from typing import Optional, List, Dict
from prometheus_client.parser import text_string_to_metric_families


async def get_sglang_metrics(
    server_url: str,
    metric_list: Optional[List[str]] = None
) -> Optional[List[Dict]]:
    """Fetch and optionally filter SGLang Prometheus metrics.
    
    Details: https://docs.sglang.ai/references/production_metrics.html
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{server_url}/metrics", timeout=3) as response:
                if response.status != 200:
                    print(f"[ERROR] Failed to fetch metrics from {server_url}, status: {response.status}")
                    return None
                metrics_text = await response.text()

        parsed_metrics = []
        for family in text_string_to_metric_families(metrics_text):
            for sample in family.samples:
                if metric_list is None or sample.name in metric_list:
                    parsed_metrics.append({
                        "name": sample.name,
                        "value": sample.value,
                        "labels": sample.labels
                    })
        return parsed_metrics

    except Exception as e:
        print(f"[ERROR] Failed to fetch metrics from {server_url}: {e}")
        return None


async def main():
    url = "http://192.168.102.15:30000"

    # all_metrics = await get_sglang_metrics(url)
    # print("All Metrics:")
    # print(all_metrics)

    filtered = await get_sglang_metrics(
        url, 
        metric_list=["sglang:token_usage", "sglang:num_queue_reqs", "sglang:num_running_reqs"]
    )
    print(filtered)


if __name__ == "__main__":
    asyncio.run(main())

