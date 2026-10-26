# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""thingagent mcp: an MCP client calls the fleet as one of the config's clients."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import BROKER, CLIENT, Rig, login
from mcp.shared.memory import create_connected_server_and_client_session as connect
from mcp.types import TextResourceContents
from pydantic import AnyUrl

from thingagent.cli import Config, serving


@pytest.mark.network
async def test_an_mcp_client_sees_and_does_only_what_its_grants_allow(
    rig: Rig, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The client may ask for door__unlock, but the fleet's gateway does not grant it.
    client = CLIENT | {"may": ["door__lock", "door__unlock"]}
    config = {
        "broker": BROKER,
        "prefix": rig.prefix,
        "fleet": "bus",
        "clients": [client],
        "token_env": {"claude": "CLAUDE_TOKEN"},
        "logins": {"claude": {"user": "claude", "password_env": "CLAUDE_PASSWORD"}},
        "trace": "trace.jsonl",
    }
    (tmp_path / "thingagent.json").write_text(json.dumps(config))
    monkeypatch.setenv("CLAUDE_TOKEN", rig.keys.token("claude"))
    monkeypatch.setenv("CLAUDE_PASSWORD", login("claude")[1])

    async with (
        serving(Config(tmp_path / "thingagent.json"), "claude") as server,
        connect(server) as session,
    ):
        await session.initialize()
        tools = {tool.name for tool in (await session.list_tools()).tools}
        resources = [str(r.uri) for r in (await session.list_resources()).resources]
        read = await session.read_resource(AnyUrl("thing://thermostat__temperature"))
        locked = await session.call_tool("door__lock", {})
        unlocked = await session.call_tool("door__unlock", {})

    # Nothing the client does not read or may not take is listed.
    assert tools == {"approve", "door__lock", "door__unlock"}
    assert resources == ["thing://thermostat__temperature"]
    assert [c.text for c in read.contents if isinstance(c, TextResourceContents)] == ["26.5"]
    assert not locked.isError
    assert rig.devices.locked
    assert unlocked.isError
    assert unlocked.structuredContent == {
        "error": "authorization denied: grant does not include "
        "('urn:demo:door', 'unlock', 'invokeaction')",
        "denied": True,
    }
    assert [r.actor for r in rig.refusals("unlock")] == ["claude"]
    records = [json.loads(line)["record"] for line in (tmp_path / "trace.jsonl").open()]
    assert [
        (r["sosa:actsOnProperty"], r["schema:actionStatus"], r["prov:wasAssociatedWith"])
        for r in records
        if "sosa:Actuation" in r["@type"]
    ] == [
        ("lock", "schema:CompletedActionStatus", "urn:thingagent:client:claude"),
        ("unlock", "schema:FailedActionStatus", "urn:thingagent:client:claude"),
    ]
