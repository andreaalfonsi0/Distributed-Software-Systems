from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a cluster demo with replication, failure, and recovery.")
    parser.add_argument("--project-dir", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--compose-file", type=Path, default=Path("docker-compose.yml"))
    parser.add_argument("--wait", type=float, default=8.0, help="Seconds to wait for recovery after restart")
    return parser.parse_args()


def compose(args: argparse.Namespace, *cmd: str) -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(args.compose_file), *cmd],
        cwd=args.project_dir,
        check=True,
    )


def wait_for_health(base_url: str, timeout: float = 45.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            response = httpx.get(f"{base_url}/health", timeout=2.0)
            response.raise_for_status()
            return
        except Exception:
            time.sleep(1)
    raise TimeoutError(f"service at {base_url} did not become healthy in time")


def upload(base_url: str, logical_path: str, content: bytes) -> dict:
    with tempfile.NamedTemporaryFile(delete=False) as handle:
        handle.write(content)
        temp_path = Path(handle.name)
    with temp_path.open("rb") as stream:
        response = httpx.post(
            f"{base_url}/files/upload",
            files={"file": (logical_path.split("/")[-1], stream, "text/plain")},
            data={"logical_path": logical_path},
            timeout=10.0,
        )
    temp_path.unlink(missing_ok=True)
    response.raise_for_status()
    return response.json()


def download_text(base_url: str, logical_path: str) -> str:
    response = httpx.get(f"{base_url}/files/{logical_path}/download", timeout=10.0)
    response.raise_for_status()
    return response.text


def main() -> int:
    args = parse_args()
    compose(args, "up", "-d", "--build")

    nodes = {
        "node1": "http://127.0.0.1:8001",
        "node2": "http://127.0.0.1:8002",
        "node3": "http://127.0.0.1:8003",
    }
    for base_url in nodes.values():
        wait_for_health(base_url)
    

    logical_path = "demo/notes.txt"
    first_version = b"version 1 from node1\n"
    second_version = b"version 2 while node3 is offline\n"

    input("Cluster is ready. Press Enter to start the demo...")
    first_result = upload(nodes["node1"], logical_path, first_version)
    print("Initial upload:", first_result)
    input("File uploaded. Press Enter to read from node2...")
    print("Replica read from node2:", download_text(nodes["node2"], logical_path).strip())

    input("Now we will stop node3 to simulate a failure. Press Enter to continue...")
    compose(args, "stop", "node3")
    print("Stopped node3")

    input("Node3 is offline. Press Enter to upload a new version while node3 is down...")
    second_result = upload(nodes["node1"], logical_path, second_version)
    print("Upload during failure:", second_result)

    input("Now we will restart node3 to simulate recovery. Press Enter to continue...")
    compose(args, "start", "node3")
    wait_for_health(nodes["node3"])
    time.sleep(args.wait)

    input("Node3 has restarted. Press Enter to read from node3 and verify recovery...")
    recovered_content = download_text(nodes["node3"], logical_path).strip()
    print("Recovered read from node3:", recovered_content)

    if recovered_content != second_version.decode("utf-8").strip():
        print("Recovery verification failed", file=sys.stderr)
        return 1

    print("Recovery verification succeeded")
    input("Demo complete. Press Enter to exit...")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
