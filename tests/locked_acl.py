# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""Write the rules a locked broker needs for the live tests, compiled as
``thingagent generate`` compiles them: each caller may send only what the tests'
gateways grant it, and each gateway may serve only its own Things, under any
prefix, since each run picks its own.

    python tests/locked_acl.py ACL_FILE
"""

from __future__ import annotations

import sys
from pathlib import Path

from conftest import FLEET, RULE_LOGINS, fleet_grants
from thingctx.gateways.mosquitto import acl_file

DOORWATCH = "urn:thingagent:agent:doorwatch"
CHAT = "urn:thingagent:channel:chat"
PLANT = "urn:demo:plant"


def rules() -> str:
    grants = {name: set(given) for name, given in fleet_grants().items()}
    # Each rule in the tests calls doorwatch's decide over the bus.
    for name in RULE_LOGINS:
        grants[name].add((DOORWATCH, "decide", "invokeaction"))
    grants |= {
        "chat": {(CHAT, "receive", "invokeaction")},
        "app": {(DOORWATCH, "decided", "subscribeevent")},
        "mallory": set(),
        "executor": {(PLANT, "open_valve", "invokeaction")},
        "agents_gw": set(),
    }
    serves = {
        "fleet_gw": [*(td["id"] for td in FLEET), PLANT],
        "agents_gw": [DOORWATCH, "urn:thingagent:agent:advisor", CHAT],
    }
    return acl_file(
        gateways=list(serves), callers=list(grants), prefix="+", grants=grants, serves=serves
    )


if __name__ == "__main__":
    Path(sys.argv[1]).write_text(rules())
