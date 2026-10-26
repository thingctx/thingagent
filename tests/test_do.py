# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""An agent that may act: it carries out a task with calls and arguments a model
chooses, bounded by the actions it may take and checked where they land, and an
action it must have approved waits for its person, who may answer from a device."""

from __future__ import annotations

import asyncio
import dataclasses
import json
from pathlib import Path
from typing import Any

import pytest
from conftest import CTX, DOORWATCH, FLEET, NOSEC, Rig, eventually
from test_hero import Script, _calls, _says, _to
from thingctx import ThingClient
from thingctx.authz import limited
from thingctx.bindings import LocalBinding

from thingagent import Agent, Rule, check, grants

LAMP = {
    "@context": CTX,
    "id": "urn:demo:lamp",
    "title": "Hall lamp",
    **NOSEC,
    "properties": {
        "knob": {
            "type": "object",
            "properties": {"pressed": {"type": "boolean"}},
            "readOnly": True,
            "forms": [{"href": "local://knob", "op": ["readproperty", "observeproperty"]}],
        }
    },
    "actions": {
        "light": {
            "input": {"type": "object", "properties": {"on": {"type": "boolean"}}},
            "output": {"type": "object"},
            "forms": [{"href": "local://light"}],
        }
    },
}
KEEPER = {
    "name": "keeper",
    "title": "House keeper",
    "instructions": "You keep the house.",
    "reads": ["door__locked"],
    "may": ["door__lock", "alarm__sound"],
    "approve": {"alarm__sound": "Sound the alarm?"},
    "approval": {
        "timeout": 5,
        "signal": [{"invoke": "lamp__light", "args": {"on": True}}],
        "clear": [{"invoke": "lamp__light", "args": {"on": False}}],
    },
    "person": "alice",
    "rounds": 4,
}
KNOB = Rule.from_dict(
    {
        "name": "knob_says_yes",
        "event": "lamp__knob",
        "when": {"pressed": True},
        "calls": "keeper__approve",
        "args": {"person": "alice", "yes": True, "action": "alarm__sound"},
    }
)


@pytest.fixture
async def house(in_process: Any) -> Any:
    """The devices, the lamp's switch history, the lamp, and a keeper factory."""
    devices, make = in_process
    switched: list[bool] = []
    binding = devices.binding()

    def light(on: bool) -> dict[str, bool]:
        switched.append(on)
        return {"on": on}

    lamp = LocalBinding({"knob": lambda: {"pressed": False}, "light": light})
    binding.register_thing("lamp", lamp)

    async def keeper(model: Script, **options: Any) -> Any:
        return await make(
            agents=[Agent.from_dict(KEEPER)],
            fleet=[*FLEET, LAMP],
            bindings=[binding],
            allow={"alice": ["keeper"]},
            chat_fn=model,
            **options,
        )

    return devices, switched, lamp, keeper


async def test_a_task_runs_the_calls_the_model_chooses(house: Any, tmp_path: Path) -> None:
    devices, _, _, keeper = house
    model = Script(_calls(("door__locked__get", {}), ("door__lock", {})), _says("Locked."))
    harness = await keeper(model, trace=tmp_path / "trace.jsonl")
    did = await harness.do(harness.agents["keeper"], "Lock the door.")
    assert devices.locked
    assert did["answer"] == "Locked."
    [acted] = did["acted"]
    assert "sosa:Actuation" in acted["@type"]
    # The model sees only what the keeper reads and may take.
    assert set(model.shown[0][1]) >= {"door__locked__get", "door__lock", "alarm__sound"}
    assert not any(name.startswith(("thermostat", "camera", "lamp")) for name in model.shown[0][1])
    lines = [json.loads(line) for line in (tmp_path / "trace.jsonl").read_text().splitlines()]
    assert [next(k for k in line if k not in ("at", "agent")) for line in lines] == [
        "task",
        "record",
        "call",
        "record",
        "call",
        "answer",
        "did",
    ]
    assert lines[4]["args"] == {} and lines[4]["call"] == "door__lock"


async def test_a_task_cannot_reach_what_the_agent_may_not_take(house: Any) -> None:
    devices, _, _, keeper = house
    model = Script(_calls(("door__unlock", {})), _says("I could not unlock it."))
    harness = await keeper(model)
    devices.locked = True
    await harness.do(harness.agents["keeper"], "Unlock the door.")
    assert devices.locked
    assert "unknown action" in model.shown[1][0][-1]["content"]


@pytest.mark.parametrize("yes", [True, False])
async def test_an_approved_action_waits_for_its_person(house: Any, yes: bool) -> None:
    devices, switched, _, keeper = house
    model = Script(_calls(("alarm__sound", {})), _says("Done."))
    harness = await keeper(model)
    task = asyncio.ensure_future(harness.do(harness.agents["keeper"], "Sound the alarm."))
    [question] = await eventually(lambda: _to("alice", harness.channel.outbox), what="the question")
    assert question.startswith("Sound the alarm? (alarm__sound")
    assert switched == [True]
    assert devices.sounded == 0
    await harness.channel.answer("alice", yes, "alarm__sound")
    did = await task
    assert devices.sounded == (1 if yes else 0)
    assert switched == [True, False]
    kinds = [entry["@type"] for entry in did["acted"]]
    assert ("sosa:Actuation" in kinds[-1]) is yes


async def test_a_device_answers_the_question_through_a_rule(house: Any) -> None:
    devices, _, lamp, keeper = house
    model = Script(_calls(("alarm__sound", {})), _says("Done."))
    harness = await keeper(model, rules=[KNOB])
    task = asyncio.ensure_future(harness.do(harness.agents["keeper"], "Sound the alarm."))
    await eventually(lambda: _to("alice", harness.channel.outbox), what="the question")
    lamp.emit("knob", {"pressed": False})
    await asyncio.sleep(0.05)
    assert devices.sounded == 0
    lamp.emit("knob", {"pressed": True})
    await task
    assert devices.sounded == 1


async def test_a_device_approves_only_the_action_its_rule_names(house: Any) -> None:
    devices, _, lamp, keeper = house
    model = Script(_calls(("alarm__sound", {})), _says("Done."))
    unlock = dataclasses.replace(KNOB, args=KNOB.args | {"action": "door__unlock"})
    harness = await keeper(model, rules=[unlock])
    task = asyncio.ensure_future(harness.do(harness.agents["keeper"], "Sound the alarm."))
    await eventually(lambda: _to("alice", harness.channel.outbox), what="the question")
    lamp.emit("knob", {"pressed": True})
    await asyncio.sleep(0.05)
    assert not task.done()
    assert devices.sounded == 0
    # The person's own reply still answers the question that stayed open.
    await harness.channel.receive("alice", "yes")
    await task
    assert devices.sounded == 1


async def test_a_press_with_no_question_open_is_not_a_message(house: Any) -> None:
    _, _, lamp, keeper = house
    harness = await keeper(Script(), rules=[KNOB])
    lamp.emit("knob", {"pressed": True})
    await asyncio.sleep(0.05)
    assert harness.channel.outbox == []


async def test_a_rule_starts_a_task(house: Any) -> None:
    devices, _, _, keeper = house
    rule = Rule.from_dict(
        {
            "name": "lock_for_strangers",
            "event": "camera__person",
            "when": {"label": "person"},
            "calls": "keeper__do",
            "args": {"task": "Someone is at the door. Lock it."},
        }
    )
    model = Script(_calls(("door__lock", {})), _says("Locked."))
    harness = await keeper(model, rules=[rule])
    devices.camera.emit("person", {"label": "person"})
    await eventually(lambda: devices.locked or None, what="the door locked")
    prompt = model.shown[0][0][-1]["content"]
    assert prompt.startswith("Someone is at the door. Lock it.")
    assert '"label": "person"' in prompt
    assert harness.channel.outbox == []


async def test_a_person_who_writes_to_an_agent_that_may_act_sets_it_a_task(house: Any) -> None:
    devices, _, _, keeper = house
    harness = await keeper(Script(_calls(("door__lock", {})), _says("Locked.")))
    await harness.channel.receive("alice", "@keeper lock the door")
    assert await eventually(lambda: _to("alice", harness.channel.outbox), what="the answer") == [
        "Locked."
    ]
    assert devices.locked


def test_an_agent_that_may_act_is_served_with_do_and_did() -> None:
    td = Agent.from_dict(KEEPER).td()
    assert td["actions"]["do"]["input"]["required"] == ["task"]
    assert set(td["events"]) == {"decided", "did"}
    assert "do" not in Agent.from_dict({"name": "quiet"}).td()["actions"]


def test_an_approval_for_an_action_the_agent_may_not_take_is_refused() -> None:
    with pytest.raises(ValueError, match="may not take"):
        Agent.from_dict({**KEEPER, "approve": {"door__unlock": "Unlock?"}})


def test_grants_cover_what_a_task_may_take_and_what_a_rule_observes() -> None:
    given = grants([Agent.from_dict(KEEPER)], [KNOB], [*FLEET, LAMP])
    assert given["keeper"] == {
        ("urn:demo:door", "locked", "readproperty"),
        ("urn:demo:door", "lock", "invokeaction"),
        ("urn:demo:alarm", "sound", "invokeaction"),
        limited("urn:demo:lamp", "light", "invokeaction", {"enum": [{"on": True}, {"on": False}]}),
    }
    assert given["knob_says_yes"] == {("urn:demo:lamp", "knob", "observeproperty")}


def test_check_reports_a_rule_call_without_its_input() -> None:
    agents = [Agent.from_dict(KEEPER)]
    rules = [
        Rule.from_dict({"name": "a", "event": "lamp__knob", "calls": "keeper__do"}),
        Rule("b", "keeper__approve", event="lamp__knob", args={"person": "alice", "yes": True}),
        Rule.from_dict({"name": "c", "event": "lamp__light", "calls": "keeper__decide"}),
    ]
    assert check(agents, rules, [*FLEET, LAMP]) == [
        "rule a calls keeper__do with no task in its args",
        "rule b calls keeper__approve without a person, a yes and an action in its args",
        "rule c reads lamp__light, which no fleet TD has as an event or an observable property",
    ]


async def test_a_task_over_the_bus_calls_as_the_agent(rig: Rig) -> None:
    agent = Agent.from_dict({**DOORWATCH, "may": ["door__lock"]})
    model = Script(_calls(("door__lock", {})), _says("Locked."))
    harness = await rig.harness(agents=[agent], chat_fn=model)
    did = await harness.do(agent, "Lock the door.")
    assert rig.devices.locked
    assert did["answer"] == "Locked."
    assert len(rig.actuations("lock")) == 1


async def test_a_task_over_the_bus_is_refused_what_the_agent_was_not_granted(rig: Rig) -> None:
    # The gateway grants doorwatch only what its table names, which unlock is not.
    agent = Agent.from_dict({**DOORWATCH, "may": ["door__unlock"]})
    rig.devices.locked = True
    model = Script(_calls(("door__unlock", {})), _says("I could not unlock it."))
    harness = await rig.harness(agents=[agent], chat_fn=model)
    await harness.do(agent, "Unlock the door.")
    assert rig.devices.locked
    assert '"error"' in model.shown[1][0][-1]["content"]


async def test_a_task_model_reads_no_correlation_id(house: Any) -> None:
    _, _, _, keeper = house
    model = Script(_says("Nothing to do."))
    harness = await keeper(model)
    trigger = {"label": "person", "correlation": "c9"}

    did = await harness.do(harness.agents["keeper"], "Check the door.", trigger=trigger)

    assert did["correlation"] == "c9"
    prompt = model.shown[0][0][-1]["content"]
    assert prompt.endswith('What started this: {"label": "person"}')
    assert "correlation" not in json.dumps(model.shown[0][0])


def test_an_agent_offers_no_cancel_tools() -> None:
    td = Agent.from_dict(KEEPER).td()

    tools = [t["name"] for t in ThingClient(tds=[td]).tool_surface()]

    assert {"keeper__ask", "keeper__do"} <= set(tools)
    assert not any(name.endswith("cancel") for name in tools)
