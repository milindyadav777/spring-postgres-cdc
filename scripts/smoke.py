#!/usr/bin/env python3
"""Exercises the running Compose stack from the repository root. Use synthetic data only."""
import json
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4


def compose(*args):
    return subprocess.check_output(["docker", "compose", *args], text=True, timeout=90).strip()


def sql(host, database, statement):
    return compose("exec", "-T", host, "psql", "-XAtq", "-v", "ON_ERROR_STOP=1",
                   "-U", "postgres", "-d", database, "-c", statement)


def http(path, data=None):
    body = json.dumps(data).encode() if data is not None else None
    request = Request(BASE + path, data=body, method="PUT" if body else "GET",
                      headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=10) as response:
            return response.status, json.load(response), response.headers
    except HTTPError as err:
        return err.code, None, err.headers


def eventually(description, test, timeout=180):
    end = time.monotonic() + timeout
    last = "condition was false"
    while time.monotonic() < end:
        try:
            if test():
                print("PASS", description, flush=True)
                return
        except (URLError, TimeoutError, ConnectionError, subprocess.SubprocessError) as err:
            last = str(err)
        time.sleep(1)
    raise AssertionError(f"{description}: {last}")

BASE = "http://127.0.0.1:8080"
eventually("consumer API ready", lambda: http("/actuator/health/readiness")[0] == 200)


def connector_running():
    with urlopen("http://127.0.0.1:8083/connectors/orders-cdc/status", timeout=5) as response:
        status = json.load(response)
    return status["connector"]["state"] == "RUNNING" and any(t["state"] == "RUNNING" for t in status["tasks"])


eventually("logical replication task running", connector_running)
eventually("initial snapshot reached projection", lambda: http("/orders/1")[0] == 200)
order = int(sql("source", "orders", "INSERT INTO orders(status) VALUES ('CREATED') RETURNING id"))
path = f"/orders/{order}"


def has_status(status):
    code, body, _ = http(path)
    return code == 200 and body["status"] == status


eventually("insert projected", lambda: has_status("CREATED"))
sql("source", "orders", f"BEGIN; UPDATE orders SET status='CANCELLED' WHERE id={order}; ROLLBACK")
compose("stop", "projector")
try:
    sql("source", "orders", f"UPDATE orders SET status='PAID' WHERE id={order}")
finally:
    compose("start", "projector")
eventually("offline consumer catches up", lambda: has_status("PAID"))
revision = int(http(path)[1]["revision"])
sql("source", "orders", f"DELETE FROM orders WHERE id={order}")
eventually("physical deletion projected", lambda: http(path)[0] == 404)
stored = sql("sink", "projection", f"SELECT revision,deleted FROM order_view WHERE id={order}")
assert stored == f"{revision}|t", stored
print("PASS delete fence persists at last row revision")
