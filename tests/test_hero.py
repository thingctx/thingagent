# Copyright 2026 The thingagent Authors
# SPDX-License-Identifier: Apache-2.0
"""The two ways an agent starts, end to end on a live broker: a person through
the chat channel, and an event through a rule. Every fleet call crosses a guarded
thingctx gateway as the agent's own identity."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
from collections.abc import Awaitable
from typing import Any

import pytest
from conftest import (
    ADVISOR,
    DOORWATCH,
    LOCKED,
    SNAPSHOT,
    Rig,
    eventually,
    publish_as,
)
from thingctx import TransportError

from thingagent import Agent, Rule, checked

pytestmark = pytest.mark.network
# The PUBACK reason a broker gives a publish it does not authorize (MQTT 5.0,
# section 3.4.2.1).
NOT_AUTHORIZED = 0x87


class Script:
    """A chat model that plays fixed turns and keeps what it was shown."""

    def __init__(self, *turns: dict[str, Any]) -> None:
        self.turns = list(turns)
        self.shown: list[tuple[list[dict[str, Any]], list[str]]] = []

    async def __call__(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> dict[str, Any]:
        self.shown.append((copy.deepcopy(messages), [t["function"]["name"] for t in tools]))
        return self.turns.pop(0)


def _calls(*calls: tuple[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": f"call_{i}",
                "type": "function",
                "function": {"name": name, "arguments": json.dumps(args)},
            }
            for i, (name, args) in enumerate(calls)
        ],
    }


def _says(text: str) -> dict[str, Any]:
    return {"role": "assistant", "content": text}


class Stranger:
    """A decider that sees a stranger every time."""

    def __init__(self) -> None:
        self.asked: list[tuple[dict[str, Any], str | None]] = []

    async def answer(
        self, agent: Agent, context: dict[str, Any], image: str | None
    ) -> dict[str, Any]:
        self.asked.append((context, image))
        answers = [checked(q, {"choice": "stranger", "confidence": 0.9}) for q in agent.questions]
        return {"answers": answers}


async def _refused(call: Awaitable[Any]) -> Any:
    """Await a call the bus must refuse. A locked broker runs the compiled rules
    and refuses the publish itself; an open broker leaves the refusal to the
    gateway, whose answer is returned."""
    if LOCKED:
        with pytest.raises(TransportError, match="refused the publish"):
            await call
        return None
    return await call


def _to(person: str, outbox: list[dict[str, str]]) -> list[str]:
    return [m["text"] for m in outbox if m["person"] == person]


async def test_a_person_asks_through_the_chat_channel(rig: Rig) -> None:
    model = Script(
        _calls(("door__locked__get", {})),
        _says("A courier is at the door, and it is unlocked."),
    )
    harness = await rig.harness(chat_fn=model)
    served = {td["id"]: td for td in harness.served_tds()}
    adapter = rig.client(served["urn:thingagent:channel:chat"], "chat", "channel")

    await adapter.invoke("chat__receive", {"person": "alice", "text": "@doorwatch who is there?"})
    told = await eventually(lambda: _to("alice", harness.channel.outbox), what="the answer")

    assert told == ["A courier is at the door, and it is unlocked."]
    messages, tools = model.shown[0]
    assert {"door__lock", "door__unlock", "alarm__sound"}.isdisjoint(tools)
    assert "door__locked__get" in tools
    assert {"type": "image_url", "image_url": {"url": SNAPSHOT}} in messages[-1]["content"]
    assert not rig.devices.locked
    assert rig.actuations("lock") == []


async def test_a_chat_message_needs_the_channel_role(rig: Rig) -> None:
    harness = await rig.harness(chat_fn=Script())
    chat = next(td for td in harness.served_tds() if td["id"] == "urn:thingagent:channel:chat")
    stranger = rig.client(chat, "mallory", "observer")

    refused = await _refused(
        stranger.invoke("chat__receive", {"person": "alice", "text": "@doorwatch hi"})
    )

    assert LOCKED or refused.get("denied") is True
    assert harness.channel.outbox == []


async def test_the_agent_token_is_what_the_fleet_authorizes(rig: Rig) -> None:
    fleet = {td["id"]: td for td in rig.fleet}
    door = rig.client(fleet["urn:demo:door"], "doorwatch", "agent")
    alarm_by_rule = rig.client(fleet["urn:demo:alarm"], "stranger", "rule")
    unlock = await _refused(door.invoke("door__unlock", {}))
    sound = await _refused(alarm_by_rule.invoke("alarm__sound", {}))
    assert LOCKED or (unlock.get("denied") is True and sound.get("denied") is True)
    assert rig.devices.sounded == 0


async def test_a_token_works_only_from_the_login_it_names(rig: Rig) -> None:
    fleet = {td["id"]: td for td in rig.fleet}
    thief = rig.client(fleet["urn:demo:door"], "doorwatch", "agent", user="mallory")

    refused = await _refused(thief.invoke("door__lock", {}))

    assert LOCKED or refused == {"error": "authentication required", "denied": True}
    assert not rig.devices.locked
    assert rig.actuations("lock") == []


@pytest.mark.skipif(not LOCKED, reason="an open broker lets any login publish at any level")
async def test_a_locked_broker_refuses_a_request_at_another_logins_level(rig: Rig) -> None:
    """A thief that writes the topic itself, instead of letting a thingctx client
    fill in its own level, is stopped by the broker: mallory may send nothing, and
    advisor, with grants of its own, still may not send as doorwatch."""
    stolen = rig.keys.token("doorwatch", "agent")
    request = f"{rig.prefix}/door/actions/lock/doorwatch"

    reasons = {thief: publish_as(thief, request, stolen) for thief in ("mallory", "advisor")}

    assert reasons == {"mallory": NOT_AUTHORIZED, "advisor": NOT_AUTHORIZED}
    assert not rig.devices.locked
    assert rig.actuations("lock") == []


async def test_a_step_outside_the_agents_grants_is_reported_not_done(rig: Rig) -> None:
    """The fleet's grants come from doorwatch's own definition. A table that names
    another action gets it refused: by the broker's compiled rules on a locked
    broker, by the gateway's policy on an open one. thingagent reports it either way."""
    reckless = {"who": {"stranger": [{"invoke": "door__unlock"}, {"invoke": "alarm__sound"}]}}
    agents = [Agent.from_dict(DOORWATCH | {"act": reckless}), Agent.from_dict(ADVISOR)]
    harness = await rig.harness(decider=Stranger(), agents=agents)

    decision = await harness.decide(harness.agents["doorwatch"], {"label": "person"})

    unlock, sound = decision["acted"]
    assert "result" in sound
    assert rig.devices.sounded == 1
    assert "result" not in unlock
    if LOCKED:
        assert "refused the publish" in unlock["failed"]
        assert rig.refusals("unlock") == []
    else:
        assert unlock["refused"]["denied"] is True
        assert [r.actor for r in rig.refusals("unlock")] == ["doorwatch"]


async def test_concurrent_asks_do_not_wait_on_each_other(rig: Rig) -> None:
    alice_waiting, bob_answered = asyncio.Event(), asyncio.Event()

    async def model(messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> dict[str, Any]:
        prompt = messages[1]["content"][0]["text"]
        if prompt == "status?":
            bob_answered.set()
            return _says("All quiet.")
        alice_waiting.set()
        await bob_answered.wait()
        return _says("A courier is at the door.")

    harness = await rig.harness(chat_fn=model, allow={"alice": ["doorwatch"], "bob": ["doorwatch"]})
    await harness.channel.receive("alice", "@doorwatch lock it for the courier")
    await asyncio.wait_for(alice_waiting.wait(), 10)
    await harness.channel.receive("bob", "@doorwatch status?")

    assert await eventually(lambda: _to("bob", harness.channel.outbox)) == ["All quiet."]
    question = await eventually(lambda: _to("alice", harness.channel.outbox))
    assert question == ["A courier is at the door."]


async def test_an_event_starts_a_decision_through_a_rule(rig: Rig) -> None:
    rule = Rule.from_dict(
        {
            "name": "person_at_door",
            "event": "camera__person",
            "when": {"label": "person"},
            "calls": "doorwatch__decide",
        }
    )
    tokens = {
        "doorwatch": rig.keys.token("doorwatch", "agent"),
        "person_at_door": rig.keys.token("person_at_door", "rule"),
    }
    decider = Stranger()
    await rig.harness(decider=decider, rules=[rule], tokens=tokens)

    rig.devices.camera.emit("person", {"label": "cat", "at": "t1"})
    rig.devices.camera.emit("person", {"label": "person", "at": "t2"})
    await eventually(lambda: rig.devices.sounded, what="the alarm")

    assert [context["trigger"] for context, _ in decider.asked] == [{"label": "person", "at": "t2"}]
    assert [a.actor for a in rig.actuations("lock")] == ["doorwatch"]


async def test_an_open_bus_runs_an_acting_agent_only_when_told_to(rig: Rig) -> None:
    with pytest.raises(ValueError, match='"open_bus": true'):
        await rig.harness(broker_binds_identity=False)
    harness = await rig.harness(
        broker_binds_identity=False, open_bus=True, guard=None, policy=None, decider=Stranger()
    )

    await harness.decide(harness.agents["doorwatch"], {"label": "person"})

    assert rig.devices.locked


async def test_an_agent_sends_a_table_step_over_the_bus_only_with_its_own_arguments(
    rig: Rig,
) -> None:
    harness = await rig.harness(decider=Stranger())

    refused = await harness._actors["doorwatch"].invoke("door__lock", {"force": True})

    assert refused == {
        "error": "authorization denied",
        "thing": "urn:demo:door",
        "affordance": "lock",
        "op": "invokeaction",
        "reason": "arguments outside the grant's limits for "
        "('urn:demo:door', 'lock', 'invokeaction'): {'force': True} is not one of [{}]",
    }
    assert not rig.devices.locked
    assert rig.actuations("lock") == []


async def test_a_restarted_rule_gets_the_events_sent_while_it_was_down(rig: Rig) -> None:
    rule = Rule.from_dict(
        {
            "name": "person_at_door",
            "event": "camera__person",
            "when": {"label": "person"},
            "calls": "doorwatch__decide",
        }
    )
    tokens = {
        "doorwatch": rig.keys.token("doorwatch", "agent"),
        "person_at_door": rig.keys.token("person_at_door", "rule"),
    }
    first = await rig.harness(decider=Stranger(), rules=[rule], tokens=tokens)
    await first.aclose()

    rig.devices.camera.emit("person", {"label": "person", "at": "t1"})
    decider = Stranger()
    await rig.harness(decider=decider, rules=[rule], tokens=tokens)
    await eventually(lambda: decider.asked, what="the event sent while down")

    assert [context["trigger"] for context, _ in decider.asked] == [{"label": "person", "at": "t1"}]


async def test_a_rule_reopens_its_stream_when_the_fleets_gateway_restarts(
    rig: Rig, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="thingagent")
    rule = Rule.from_dict(
        {
            "name": "person_at_door",
            "event": "camera__person",
            "when": {"label": "person"},
            "calls": "doorwatch__decide",
        }
    )
    tokens = {
        "doorwatch": rig.keys.token("doorwatch", "agent"),
        "person_at_door": rig.keys.token("person_at_door", "rule"),
    }
    decider = Stranger()
    await rig.harness(decider=decider, rules=[rule], tokens=tokens)

    await rig.restart_gateway()
    await eventually(lambda: "rule person_at_door reopened" in caplog.text, what="the reopen")
    rig.devices.camera.emit("person", {"label": "person", "at": "t1"})
    await eventually(lambda: decider.asked, what="the event after the restart")

    assert "camera__person was served again; reopening" in caplog.text
    assert [context["trigger"] for context, _ in decider.asked] == [{"label": "person", "at": "t1"}]
