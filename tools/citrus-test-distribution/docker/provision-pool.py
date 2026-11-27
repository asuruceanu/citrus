# Copyright the original author or authors.
# Licensed under the Apache License, Version 2.0.

"""Provision the local Compose pool before its worker replicas start."""

import json
import os
from pathlib import Path
from urllib.request import Request, urlopen

POOL_NAME = "Local Docker"
# The coordinator matches partition requirements against both the pool and its workers.
POOL_CAPABILITIES = {
    "jdk": "21",
    "citrus.workspace.v1": "true",
    "citrus.workspace.jvm-layout.v1": "true",
    "citrus.workspace.jvm-options.v1": "true",
    "citrus.workspace.quarkus.v1": "true",
    "citrus-worker": "true",
}
CREDENTIALS = Path("/registration/credentials")


def api(path="", body=None, method=None):
    headers = {"Content-Type": "application/json"}
    token = os.environ.get("CITRUS_DIST_AUTH_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request("http://coordinator:8765/api/v1/agent-pools" + path,
                      data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def provision():
    matches = [pool for pool in api() if pool["displayName"] == POOL_NAME]
    if len(matches) > 1:
        raise SystemExit("Multiple pools are named Local Docker. Give each pool a unique name.")
    if matches:
        pool_id = matches[0]["poolId"]
        if not matches[0]["capabilities"]:
            api("/" + pool_id, {"displayName": POOL_NAME, "capabilities": POOL_CAPABILITIES}, method="PUT")
    else:
        pool_id = api(body={"displayName": POOL_NAME, "capabilities": POOL_CAPABILITIES,
                            "generateRegistrationKey": False})["pool"]["poolId"]

    if CREDENTIALS.exists():
        saved = CREDENTIALS.read_text().splitlines()
        if len(saved) == 3 and saved[0] == pool_id and saved[2]:
            keys = api("/" + pool_id)["registrationKeys"]
            if any(key["keyId"] == saved[1] and not key.get("revokedAt") for key in keys):
                return

    key = api(f"/{pool_id}/keys", {"description": "Local Docker Compose workers"})
    temporary = CREDENTIALS.with_suffix(".tmp")
    os.umask(0o077)
    temporary.write_text(f"{pool_id}\n{key['metadata']['keyId']}\n{key['secretKey']}\n")
    os.chown(temporary, 10001, 10001)
    temporary.replace(CREDENTIALS)


if __name__ == "__main__":
    provision()
