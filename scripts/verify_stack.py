"""Non-mutating host-side health and data-path checks for the running Docker stack."""

import argparse
import json
import time
from urllib.error import URLError, HTTPError
from urllib.request import urlopen


def get(base: str, endpoint: str):
    with urlopen(base.rstrip("/") + endpoint, timeout=6) as response:
        return json.load(response)


def main():
    p = argparse.ArgumentParser(description="Verify the Kafka/Spark/DB/API Lambda demo")
    p.add_argument("--url", default="http://localhost:8000")
    p.add_argument("--wait-seconds", type=int, default=120)
    p.add_argument("--require-events", action="store_true")
    args = p.parse_args()
    deadline = time.monotonic() + args.wait_seconds
    while True:
        try:
            health = get(args.url, "/health")
            status = get(args.url, "/api/status")
            metrics = get(args.url, "/api/metrics")
            if health["status"] != "ok":
                raise RuntimeError("API health failure")
            complete = (status["sensor_observations"] > 0 and status["forecasts"] > 0
                        and status["batch_ready"] and status["spark_speed_latest"] is not None)
            if complete or not args.require_events:
                print("API HEALTH: OK")
                print(json.dumps({"status": status, "metrics": metrics}, indent=2))
                if args.require_events:
                    print("END-TO-END DATA CHECK: PASS")
                else:
                    print("Use --require-events to also require batch, streaming and forecast outputs")
                return
            print("Waiting for Kafka/Spark/batch/predictor events...", flush=True)
        except (HTTPError, URLError, TimeoutError, ConnectionError) as error:
            print(f"Waiting for API: {error}", flush=True)
        if time.monotonic() >= deadline:
            raise SystemExit("Timed out waiting for stack; inspect `docker compose logs` for the failing service")
        time.sleep(3)


if __name__ == "__main__":
    main()
