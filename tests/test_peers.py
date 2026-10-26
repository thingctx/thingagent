# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""Harnesses on one bus: each keeps its own names, reads no harness's agents as
fleet, and its rules call its own agents through its agents' gateway."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pytest
from conftest import (
    ADVISOR,
    AGENTS_POLICY,
    BROKER,
    DOORWATCH,
    Rig,
    eventually,
    login,
)
from test_hero import Script, Stranger, _says

from thingagent import Agent, Harness, Rule
from thingagent.cli import Config, fleet_from_bus

# The fleet's gateway grants the rule stranger the camera's person event.
CALLS_DOORWATCH = {
    "name": "stranger",
    "event": "camera__person",
    "when": {"label": "person"},
    "calls": "doorwatch__decide",
}


def _ids(harness: Harness) -> set[str]:
    return {td["id"] for td in harness.own_tds()}


def test_each_harness_names_its_own_channel(tmp_path: Path) -> None:
    agents = [Agent.from_dict(ADVISOR)]
    alone = Harness(agents, fleet=[])
    a, b = (Harness(agents, fleet=[], name=name) for name in ("a", "b"))

    assert "urn:thingagent:channel:chat" in _ids(alone)
    assert _ids(a) ^ _ids(b) == {"urn:thingagent:channel:chat_a", "urn:thingagent:channel:chat_b"}

    (tmp_path / "advisor.json").write_text(json.dumps(ADVISOR))
    config = {
        "broker": "127.0.0.1:1883",
        "fleet": "bus",
        "agents": ["advisor.json"],
        "login": {"user": "agents_gw", "password_env": "UNUSED"},
    }
    path = tmp_path / "thingagent.json"
    path.write_text(json.dumps(config))
    assert "urn:thingagent:channel:chat_agents_gw" in {td["id"] for td in Config(path).own()}
    path.write_text(json.dumps({**config, "name": "house"}))
    assert "urn:thingagent:channel:chat_house" in {td["id"] for td in Config(path).own()}


NOTARY = {
    "name": "notary",
    "title": "Notary",
    "instructions": "You note each decision.",
    "may": ["alarm__sound"],
}


@pytest.mark.network
async def test_the_fleet_on_the_bus_leaves_out_every_harness(rig: Rig) -> None:
    await rig.harness(name="a")

    fleet = await fleet_from_bus(BROKER, rig.prefix, login=login("agents_gw"))

    ids = {td["id"] for td in fleet}
    assert {td["id"] for td in rig.fleet} <= ids
    assert not any(i.startswith("urn:thingagent:") for i in ids)


def _emitted(harness: Harness, agent: str) -> list[dict[str, Any]]:
    """Every event ``agent`` emits in ``harness``, from now on."""
    seen: list[dict[str, Any]] = []
    binding = harness._bindings[agent]
    emit = binding.emit

    def spy(event: str, data: dict[str, Any]) -> Any:
        seen.append({"event": event, **data})
        return emit(event, data)

    binding.emit = spy  # type: ignore[method-assign]
    return seen


@pytest.mark.network
async def test_a_rule_calls_its_own_harness_through_the_agents_gateway(
    rig: Rig, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="thingagent")
    decider = Stranger()
    tokens = {
        "doorwatch": rig.keys.token("doorwatch", "agent"),
        "stranger": rig.keys.token("stranger", "rule"),
    }
    await rig.harness(
        agents=[Agent.from_dict(DOORWATCH)],
        tokens=tokens,
        rules=[Rule.from_dict(CALLS_DOORWATCH)],
        policy={**AGENTS_POLICY, "rule": set()},
        decider=decider,
    )

    rig.devices.camera.emit("person", {"label": "person", "at": "t1"})
    await eventually(lambda: "rule stranger was refused" in caplog.text, what="a refusal")

    assert decider.asked == []
    assert not rig.devices.locked


@pytest.mark.network
async def test_a_rule_sets_its_own_harness_a_task_over_the_bus_with_the_trigger(rig: Rig) -> None:
    notary = "urn:thingagent:agent:notary"
    rule = Rule.from_dict(
        CALLS_DOORWATCH | {"calls": "notary__do", "args": {"task": "Note who came."}}
    )
    model = Script(_says("Noted."))
    harness = await rig.harness(
        agents=[Agent.from_dict(NOTARY)],
        tokens={
            "notary": rig.keys.token("notary", "agent"),
            "stranger": rig.keys.token("stranger", "rule"),
        },
        rules=[rule],
        policy={**AGENTS_POLICY, "rule": {(notary, "do", "invokeaction")}},
        chat_fn=model,
    )
    did = _emitted(harness, "notary")

    rig.devices.camera.emit("person", {"label": "person", "at": "t1"})
    await eventually(lambda: did, what="notary's did")

    prompt = model.shown[0][0][-1]["content"]
    assert prompt.startswith("Note who came.")
    assert '"at": "t1"' in prompt
